// FFTTriggerController.cs
//
// AHM FFT Trigger Controller (v2.2.0) — RULE ONLY
// - Reads operator_status.json (from Operator Zone Engine v2.1.0)
// - When operator_zone ENTERS Zone C/D, it:
//     1) acquires exclusive lock
//     2) stops RMS poller service (exclusive VSE access)
//     3) pre-FFT quick overall read (short raw stream -> RMS)
//     4) captures FFT raw samples (time series) for C1/C2 (+C3 if VSE2 enabled)
//     5) post-FFT quick overall read
//     6) writes event marker JSON
//     7) restarts RMS poller
//
// Notes:
// - No Newtonsoft / no System.Text.Json => Mono/mcs friendly
// - Captures RAW CSV (idx,x,z). FFT spectrum can be computed elsewhere later.
// - Mapping (same intent as RMS Poller v2.0.6):
//     VSE1 sensor[0..3] => C1(X,Z) + C2(X,Z)
//     VSE2 sensor[0..1] => C3(X,Z)
//
// Build example:
//   mcs -langversion:latest -out:build/fft_trigger_controller.exe \
//       -r:../VSEUtilities_API.dll FFTTriggerController.cs
//
// Run example:
//   export LD_LIBRARY_PATH=/home/sdc/yg/work/chiller/FFT/VSEUtilities_Linux/build-VSEUtilities_Linux/src/library:$LD_LIBRARY_PATH
//   mono build/fft_trigger_controller.exe
//

using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Diagnostics;
using System.Collections.Generic;
using System.Linq;
using System.Globalization;

using vseUtilities;

public class FFTTriggerController
{
    // =========================
    // VERSION
    // =========================
    private const string VERSION = "2.2.0";

    // =========================
    // VSE CONFIG (match RMS Poller v2.0.6)
    // =========================
    private const string IpVse1 = "192.168.1.101";
    private const short PortVse1 = 3321;

    private const bool EnableVse2 = true;   // set true for 3-compressor chillers
    private const string IpVse2 = "192.168.1.102";
    private const short PortVse2 = 3321;

    // =========================
    // INPUTS from v2.1.0 Operator Zone Engine
    // =========================
    private const string OperatorStatusJson =
        "/home/sdc/yg/chiller/ahm-runtime/operator_zone/operator_status.json";

    // =========================
    // RUNTIME DIRS
    // =========================
    private const string RuntimeRoot = "/home/sdc/yg/chiller/ahm-runtime/fft_trigger";
    private static readonly string LockDir = Path.Combine(RuntimeRoot, "lock");
    private static readonly string EventsDir = Path.Combine(RuntimeRoot, "events");
    private static readonly string FftRawRoot = Path.Combine(RuntimeRoot, "fft_raw");
    private static readonly string LockFile = Path.Combine(LockDir, "vse_exclusive.lock");

    // =========================
    // EXCLUSIVE CONTROL OF RMS POLLER
    // =========================
    // IMPORTANT: set this to the real service name on AHM.
    // Example: "ahm-rms-poller.service"
    private const string RmsPollerServiceName = "ahm-rms-poller.service";

    // =========================
    // TRIGGER RULES
    // =========================
    // Trigger when operator_zone ENTERS C or D (persistence is upstream)
    private static readonly HashSet<string> TriggerZones =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "C", "D" };

    // Cooldown prevents retrigger spam if zone stays in C/D
    private const int CooldownSeconds = 10 * 60;

    // Poll operator_status.json
    private const int PollIntervalMs = 1000;

    // =========================
    // PRE/POST quick read parameters
    // =========================
    private const int PrePostWindowMs = 2000;   // 2s sampling window
    private const double RAW_TO_MMPS = 1.0;     // adjust if your raw scale already in mm/s
    private const string OverallMode = "MAX_AXIS"; // "RSS" or "MAX_AXIS"

    // =========================
    // FFT RAW CAPTURE parameters (align with your POC / production intent)
    // =========================
    private const int FftSampleRate = 100000;   // informational (raw capture)
    private const int FftSampleSize = 131072;   // required sample count per axis
    private const int FftDecimate = 2;

    // =========================
    // VSE HANDLES
    // =========================
    private static IVse _vse1;
    private static IVse _vse2;

    private static IRawDataAnReSa _rawVse1;
    private static IRawDataAnReSa _rawVse2;

    // =========================
    // STATE
    // =========================
    private static string _lastOperatorZone = "A";
    private static DateTime _lastTriggerUtc = DateTime.MinValue;

    // =========================
    // DATA STRUCTS (NO JObject)
    // =========================
    private struct RmsPack
    {
        public double RmsX;
        public double RmsZ;
        public double Overall;
        public int N;
    }

    private struct QuickReadResult
    {
        public double Overall;
        public RmsPack C1;
        public RmsPack C2;
        public RmsPack C3;
    }

    private class OperatorStatus
    {
        public string OperatorZone { get; set; }
        public string InstantZone { get; set; }
        public string StatusRowTs { get; set; }
    }

    public static int Main(string[] args)
    {
        Console.WriteLine($"=== AHM FFT Trigger Controller v{VERSION} ===");
        Console.WriteLine($"OperatorStatusJson : {OperatorStatusJson}");
        Console.WriteLine($"RuntimeRoot        : {RuntimeRoot}");
        Console.WriteLine($"EnableVse2         : {EnableVse2}");
        Console.WriteLine($"RmsPollerService   : {RmsPollerServiceName}");
        Console.WriteLine($"FFT sampleSize     : {FftSampleSize}, decimate={FftDecimate}, fs={FftSampleRate}");

        EnsureDirs();

        // Validate VSEUtilities loadability (libVSEUtilities.so must be visible via LD_LIBRARY_PATH)
        var check = ICore.checkInstanceValidity();
        if (!check.isOk())
        {
            Console.WriteLine("VSEUtilities invalid: " + check.text());
            return 2;
        }

        while (true)
        {
            try
            {
                var status = TryReadOperatorStatus();
                if (status == null)
                {
                    Thread.Sleep(PollIntervalMs);
                    continue;
                }

                string opZone = (status.OperatorZone ?? "A").Trim().ToUpperInvariant();
                string instantZone = (status.InstantZone ?? "A").Trim().ToUpperInvariant();

                bool inCooldown = _lastTriggerUtc != DateTime.MinValue &&
                                  (DateTime.UtcNow - _lastTriggerUtc).TotalSeconds < CooldownSeconds;

                bool enteringTrigger = TriggerZones.Contains(opZone) && !TriggerZones.Contains(_lastOperatorZone);

                if (enteringTrigger && !inCooldown)
                {
                    Console.WriteLine($"[TRIGGER] operator_zone entered {opZone} (from {_lastOperatorZone}). Starting FFT workflow...");
                    RunFftWorkflow(status, opZone, instantZone);
                    _lastTriggerUtc = DateTime.UtcNow;
                }

                _lastOperatorZone = opZone;
            }
            catch (Exception ex)
            {
                Console.WriteLine("[LoopError] " + ex);
            }

            Thread.Sleep(PollIntervalMs);
        }
    }

    // =========================================================
    // WORKFLOW
    // =========================================================
    private static void RunFftWorkflow(OperatorStatus status, string opZone, string instantZone)
    {
        string eventId = DateTime.Now.ToString("yyyyMMdd_HHmmss");
        string dayDir = Path.Combine(FftRawRoot, DateTime.Now.ToString("yyyy-MM-dd"));
        Directory.CreateDirectory(dayDir);

        string eventJsonPath = Path.Combine(EventsDir, $"event_{eventId}.json");

        FileStream lockHandle = null;

        try
        {
            // 1) Acquire lock (prevents multiple instances from double-triggering)
            lockHandle = AcquireExclusiveLock();

            // 2) Stop RMS poller to ensure exclusive VSE access
            Systemctl("stop", RmsPollerServiceName);

            // 3) Pre-FFT overall read
            var pre = QuickOverallRead();

            // 4) FFT capture raw samples
            var fftFiles = new List<string>();

            string c1, c2;
            if (CaptureFftVse1(dayDir, eventId, out c1, out c2))
            {
                if (!string.IsNullOrEmpty(c1)) fftFiles.Add(c1);
                if (!string.IsNullOrEmpty(c2)) fftFiles.Add(c2);
            }

            if (EnableVse2)
            {
                string c3;
                if (CaptureFftVse2(dayDir, eventId, out c3))
                {
                    if (!string.IsNullOrEmpty(c3)) fftFiles.Add(c3);
                }
            }

            // 5) Post-FFT overall read
            var post = QuickOverallRead();

            // 6) Write event marker JSON (manual JSON, deterministic)
            File.WriteAllText(eventJsonPath, BuildEventJson(eventId, status, opZone, instantZone, pre, post, fftFiles));

            Console.WriteLine("[EVENT] " + eventJsonPath);

            // 7) Resume RMS poller
            Systemctl("start", RmsPollerServiceName);

            Console.WriteLine("[DONE] FFT workflow complete.");
        }
        catch (Exception ex)
        {
            Console.WriteLine("[WorkflowError] " + ex);

            // best-effort: resume RMS poller
            try { Systemctl("start", RmsPollerServiceName); } catch { }
        }
        finally
        {
            SafeStopAll();
            try { lockHandle?.Dispose(); } catch { }
        }
    }

    private static string BuildEventJson(
        string eventId,
        OperatorStatus status,
        string opZone,
        string instantZone,
        QuickReadResult pre,
        QuickReadResult post,
        List<string> fftFiles)
    {
        var sb = new StringBuilder(2048);
        sb.AppendLine("{");
        sb.AppendLine($"  \"ts\": \"{NowIsoLocal()}\",");
        sb.AppendLine($"  \"version\": \"{VERSION}\",");
        sb.AppendLine($"  \"event_id\": \"{eventId}\",");
        sb.AppendLine($"  \"operator_zone\": \"{EscapeJson(opZone)}\",");
        sb.AppendLine($"  \"instant_zone\": \"{EscapeJson(instantZone)}\",");
        sb.AppendLine($"  \"status_row_ts\": \"{EscapeJson(status.StatusRowTs ?? "")}\",");

        sb.AppendLine($"  \"pre_overall_mmps\": {Fmt(pre.Overall)},");
        sb.AppendLine($"  \"post_overall_mmps\": {Fmt(post.Overall)},");

        sb.AppendLine("  \"pre\": {");
        sb.AppendLine($"    \"c1\": {RmsPackJson(pre.C1)},");
        sb.AppendLine($"    \"c2\": {RmsPackJson(pre.C2)},");
        sb.AppendLine($"    \"c3\": {RmsPackJson(pre.C3)}");
        sb.AppendLine("  },");

        sb.AppendLine("  \"post\": {");
        sb.AppendLine($"    \"c1\": {RmsPackJson(post.C1)},");
        sb.AppendLine($"    \"c2\": {RmsPackJson(post.C2)},");
        sb.AppendLine($"    \"c3\": {RmsPackJson(post.C3)}");
        sb.AppendLine("  },");

        sb.AppendLine("  \"fft_files\": [");
        for (int i = 0; i < fftFiles.Count; i++)
        {
            sb.Append("    \"").Append(EscapeJson(fftFiles[i])).Append("\"");
            if (i < fftFiles.Count - 1) sb.Append(",");
            sb.AppendLine();
        }
        sb.AppendLine("  ],");

        sb.AppendLine("  \"source\": \"ahm_local\"");
        sb.AppendLine("}");
        return sb.ToString();
    }

    private static string RmsPackJson(RmsPack p)
    {
        return "{ " +
               $"\"rms_x\": {Fmt(p.RmsX)}, " +
               $"\"rms_z\": {Fmt(p.RmsZ)}, " +
               $"\"overall\": {Fmt(p.Overall)}, " +
               $"\"n\": {p.N}" +
               " }";
    }

    // =========================================================
    // QUICK OVERALL READ (pre/post) — short RMS window
    // =========================================================
    private static QuickReadResult QuickOverallRead()
    {
        EnsureVseConnected();

        // Configure “AnReSa” parameters (similar intent to RMS poller)
        ConfigureVseForRead(decimate: 2);

        var untilUtc = DateTime.UtcNow.AddMilliseconds(PrePostWindowMs);

        var bufC1 = new List<(float x, float z)>();
        var bufC2 = new List<(float x, float z)>();
        var bufC3 = new List<(float x, float z)>();

        // Start VSE1 stream
        var col1 = new RawCollector(
            onVse1: (s1, s2, s3, s4) =>
            {
                bufC1.Add((s1, s2));
                bufC2.Add((s3, s4));
            },
            onVse2: null);

        _rawVse1 = new IRawDataAnReSa();
        var st1 = _rawVse1.start(_vse1, col1, null, null);
        if (!st1.isOk())
            throw new Exception("QuickOverallRead VSE1 start failed: " + st1.text());

        // Start VSE2 stream (if enabled)
        if (EnableVse2 && _vse2 != null)
        {
            var col2 = new RawCollector(
                onVse1: null,
                onVse2: (s1, s2) =>
                {
                    bufC3.Add((s1, s2));
                });

            _rawVse2 = new IRawDataAnReSa();
            var st2 = _rawVse2.start(_vse2, col2, null, null);
            if (!st2.isOk())
            {
                Console.WriteLine("QuickOverallRead VSE2 start failed: " + st2.text());
                try { _rawVse2.stop(); } catch { }
                _rawVse2 = null;
            }
        }

        while (DateTime.UtcNow < untilUtc)
            Thread.Sleep(20);

        try { _rawVse1?.stop(); } catch { }
        try { _rawVse2?.stop(); } catch { }

        var c1 = ComputeRms(bufC1);
        var c2 = ComputeRms(bufC2);
        var c3 = ComputeRms(bufC3);

        double overall = EnableVse2
            ? Math.Max(c1.Overall, Math.Max(c2.Overall, c3.Overall))
            : Math.Max(c1.Overall, c2.Overall);

        return new QuickReadResult
        {
            Overall = overall,
            C1 = c1,
            C2 = c2,
            C3 = c3
        };
    }

    private static RmsPack ComputeRms(List<(float x, float z)> buf)
    {
        if (buf == null || buf.Count < 5)
            return new RmsPack { RmsX = 0, RmsZ = 0, Overall = 0, N = (buf == null ? 0 : buf.Count) };

        double[] xs = buf.Select(p => (double)p.x).ToArray();
        double[] zs = buf.Select(p => (double)p.z).ToArray();

        double rmsX = Math.Sqrt(xs.Select(v => v * v).Average()) * RAW_TO_MMPS;
        double rmsZ = Math.Sqrt(zs.Select(v => v * v).Average()) * RAW_TO_MMPS;

        double overall = (OverallMode == "RSS")
            ? Math.Sqrt(rmsX * rmsX + rmsZ * rmsZ)
            : Math.Max(rmsX, rmsZ);

        return new RmsPack { RmsX = rmsX, RmsZ = rmsZ, Overall = overall, N = buf.Count };
    }

    // =========================================================
    // FFT RAW CAPTURE
    // =========================================================
    private static bool CaptureFftVse1(string dayDir, string eventId, out string fileC1, out string fileC2)
    {
        fileC1 = null;
        fileC2 = null;

        EnsureVseConnected();
        ConfigureVseForRead(decimate: FftDecimate);

        var c1x = new List<float>(FftSampleSize);
        var c1z = new List<float>(FftSampleSize);
        var c2x = new List<float>(FftSampleSize);
        var c2z = new List<float>(FftSampleSize);

        bool done = false;

        var col = new RawCollector(
            onVse1: (s1, s2, s3, s4) =>
            {
                if (c1x.Count < FftSampleSize) { c1x.Add(s1); c1z.Add(s2); }
                if (c2x.Count < FftSampleSize) { c2x.Add(s3); c2z.Add(s4); }
                if (c1x.Count >= FftSampleSize && c2x.Count >= FftSampleSize) done = true;
            },
            onVse2: null);

        _rawVse1 = new IRawDataAnReSa();
        var st = _rawVse1.start(_vse1, col, null, null);
        if (!st.isOk())
        {
            Console.WriteLine("FFT VSE1 start failed: " + st.text());
            return false;
        }

        var startUtc = DateTime.UtcNow;
        while (!done && (DateTime.UtcNow - startUtc).TotalSeconds < 15)
            Thread.Sleep(5);

        try { _rawVse1.stop(); } catch { }

        if (!done)
        {
            Console.WriteLine("FFT VSE1 did not collect enough samples in time.");
            return false;
        }

        fileC1 = Path.Combine(dayDir, $"FFT_{eventId}_C1_raw.csv");
        fileC2 = Path.Combine(dayDir, $"FFT_{eventId}_C2_raw.csv");
        WriteRawCsv(fileC1, c1x, c1z);
        WriteRawCsv(fileC2, c2x, c2z);

        Console.WriteLine("[FFT] VSE1 saved: " + fileC1);
        Console.WriteLine("[FFT] VSE1 saved: " + fileC2);
        return true;
    }

    private static bool CaptureFftVse2(string dayDir, string eventId, out string fileC3)
    {
        fileC3 = null;

        if (!EnableVse2) return false;

        EnsureVseConnected();
        if (_vse2 == null) return false;

        ConfigureVseForRead(decimate: FftDecimate);

        var c3x = new List<float>(FftSampleSize);
        var c3z = new List<float>(FftSampleSize);

        bool done = false;

        var col = new RawCollector(
            onVse1: null,
            onVse2: (s1, s2) =>
            {
                if (c3x.Count < FftSampleSize) { c3x.Add(s1); c3z.Add(s2); }
                if (c3x.Count >= FftSampleSize) done = true;
            });

        _rawVse2 = new IRawDataAnReSa();
        var st = _rawVse2.start(_vse2, col, null, null);
        if (!st.isOk())
        {
            Console.WriteLine("FFT VSE2 start failed: " + st.text());
            return false;
        }

        var startUtc = DateTime.UtcNow;
        while (!done && (DateTime.UtcNow - startUtc).TotalSeconds < 15)
            Thread.Sleep(5);

        try { _rawVse2.stop(); } catch { }

        if (!done)
        {
            Console.WriteLine("FFT VSE2 did not collect enough samples in time.");
            return false;
        }

        fileC3 = Path.Combine(dayDir, $"FFT_{eventId}_C3_raw.csv");
        WriteRawCsv(fileC3, c3x, c3z);

        Console.WriteLine("[FFT] VSE2 saved: " + fileC3);
        return true;
    }

    private static void WriteRawCsv(string path, List<float> xs, List<float> zs)
    {
        var sb = new StringBuilder(xs.Count * 16);
        sb.AppendLine("idx,x,z");
        int n = Math.Min(xs.Count, zs.Count);
        for (int i = 0; i < n; i++)
        {
            sb.Append(i).Append(',')
              .Append(xs[i].ToString("G9", CultureInfo.InvariantCulture)).Append(',')
              .Append(zs[i].ToString("G9", CultureInfo.InvariantCulture)).Append('\n');
        }
        File.WriteAllText(path, sb.ToString());
    }

    // =========================================================
    // VSE PARAMS (match RMS “style”: HP2Hz + IEPE)
    // =========================================================
    private static void ConfigureVseForRead(int decimate)
    {
        // VSE1 uses sensor[0..3]
        var p1 = BuildParams_HP2Hz_IEPE(decimate: decimate, use0123: true);
        var wr1 = p1.writeToDevice(_vse1);
        if (!wr1.isOk()) throw new Exception("VSE1 params write failed: " + wr1.text());

        if (EnableVse2 && _vse2 != null)
        {
            // VSE2 uses sensor[0..1]
            var p2 = BuildParams_HP2Hz_IEPE(decimate: decimate, use0123: false);
            var wr2 = p2.writeToDevice(_vse2);
            if (!wr2.isOk()) Console.WriteLine("VSE2 params write failed: " + wr2.text());
        }
    }

    private static IParametersAnReSa BuildParams_HP2Hz_IEPE(int decimate, bool use0123)
    {
        var setup = new IParametersAnReSa.Setup();

        // Default: configure 0..3 as HP2Hz + IEPE
        for (int i = 0; i < 4; i++)
        {
            setup.sensor[i].mode = IParametersAnReSa.Setup.Sensor.Mode.HP2Hz;
            setup.sensor[i].scale = 490.5f;
            setup.sensor[i].offset = 0;
            setup.sensor[i].isIEPE = true;
        }

        if (!use0123)
        {
            // for VSE2: only 0..1 on; 2..3 off
            setup.sensor[2].mode = IParametersAnReSa.Setup.Sensor.Mode.Off;
            setup.sensor[3].mode = IParametersAnReSa.Setup.Sensor.Mode.Off;
        }

        if (decimate < 1 || decimate > 255)
            throw new ArgumentOutOfRangeException(nameof(decimate), "decimate must be 1..255");

        setup.decimate = (byte)decimate;

        var parameters = IParametersAnReSa.create();
        parameters.setup(setup);
        return parameters;
    }

    // =========================================================
    // VSE CONNECTION / STOP
    // =========================================================
    private static void EnsureVseConnected()
    {
        if (_vse1 == null)
        {
            _vse1 = new IVse();
            var conn = _vse1.connect(IpVse1, PortVse1);
            if (!conn.isOk()) throw new Exception("VSE1 connect failed: " + conn.text());
        }

        if (EnableVse2 && _vse2 == null)
        {
            _vse2 = new IVse();
            var conn = _vse2.connect(IpVse2, PortVse2);
            if (!conn.isOk())
            {
                Console.WriteLine("VSE2 connect failed: " + conn.text());
                try { _vse2.disconnect(); } catch { }
                _vse2 = null;
            }
        }
    }

    private static void SafeStopAll()
    {
        try { _rawVse1?.stop(); } catch { }
        _rawVse1 = null;

        try { _rawVse2?.stop(); } catch { }
        _rawVse2 = null;

        try { _vse1?.disconnect(); } catch { }
        _vse1 = null;

        try { _vse2?.disconnect(); } catch { }
        _vse2 = null;
    }

    // =========================================================
    // OPERATOR STATUS READER (manual JSON key extraction)
    // =========================================================
    private static OperatorStatus TryReadOperatorStatus()
    {
        try
        {
            if (!File.Exists(OperatorStatusJson))
                return null;

            string json = File.ReadAllText(OperatorStatusJson);

            string op = ExtractJsonValue(json, "operator_zone") ?? "A";
            string instant = ExtractJsonValue(json, "instant_zone") ?? "A";
            string ts = ExtractJsonValue(json, "status_row_ts");

            return new OperatorStatus
            {
                OperatorZone = op,
                InstantZone = instant,
                StatusRowTs = ts
            };
        }
        catch
        {
            return null;
        }
    }

    private static string ExtractJsonValue(string json, string key)
    {
        // Very small deterministic extractor for:  "key": "value"
        string token = $"\"{key}\"";
        int idx = json.IndexOf(token, StringComparison.Ordinal);
        if (idx < 0) return null;

        int colon = json.IndexOf(':', idx);
        if (colon < 0) return null;

        int startQuote = json.IndexOf('"', colon + 1);
        if (startQuote < 0) return null;

        int start = startQuote + 1;
        int end = json.IndexOf('"', start);
        if (end < 0) return null;

        return json.Substring(start, end - start);
    }

    // =========================================================
    // LOCKING
    // =========================================================
    private static FileStream AcquireExclusiveLock()
    {
        Directory.CreateDirectory(LockDir);
        return new FileStream(LockFile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
    }

    // =========================================================
    // SYSTEMD CONTROL
    // =========================================================
    private static void Systemctl(string action, string serviceName)
    {
        RunCmd("/bin/systemctl", $"{action} {serviceName}");
    }

    private static void RunCmd(string file, string args)
    {
        var p = new Process();
        p.StartInfo.FileName = file;
        p.StartInfo.Arguments = args;
        p.StartInfo.RedirectStandardOutput = true;
        p.StartInfo.RedirectStandardError = true;
        p.StartInfo.UseShellExecute = false;
        p.StartInfo.CreateNoWindow = true;

        p.Start();
        string o = p.StandardOutput.ReadToEnd();
        string e = p.StandardError.ReadToEnd();
        p.WaitForExit();

        if (!string.IsNullOrWhiteSpace(o)) Console.WriteLine(o.Trim());
        if (!string.IsNullOrWhiteSpace(e)) Console.WriteLine(e.Trim());

        if (p.ExitCode != 0)
            throw new Exception($"Command failed ({p.ExitCode}): {file} {args}");
    }

    // =========================================================
    // UTILS
    // =========================================================
    private static void EnsureDirs()
    {
        Directory.CreateDirectory(RuntimeRoot);
        Directory.CreateDirectory(LockDir);
        Directory.CreateDirectory(EventsDir);
        Directory.CreateDirectory(FftRawRoot);
    }

    private static string NowIsoLocal()
    {
        return DateTime.Now.ToString("yyyy-MM-ddTHH:mm:ss+08:00");
    }

    private static string EscapeJson(string s)
    {
        if (s == null) return "";
        return s.Replace("\\", "\\\\").Replace("\"", "\\\"");
    }

    private static string Fmt(double v)
    {
        return v.ToString("G12", CultureInfo.InvariantCulture);
    }

    // =========================================================
    // RAW COLLECTOR (vseUtilities callback)
    // =========================================================
    public class RawCollector : IRawDataAnReSaListener
    {
        private readonly Action<float, float, float, float> _onVse1;
        private readonly Action<float, float> _onVse2;

        public RawCollector(Action<float, float, float, float> onVse1, Action<float, float> onVse2)
        {
            _onVse1 = onVse1;
            _onVse2 = onVse2;
        }

        public override void onRawData(
            long timestamp,
            float valueSensor1, float valueSensor2, float valueSensor3, float valueSensor4,
            byte valueInput1, byte valueInput2)
        {
            if (_onVse1 != null)
                _onVse1(valueSensor1, valueSensor2, valueSensor3, valueSensor4);
            else if (_onVse2 != null)
                _onVse2(valueSensor1, valueSensor2);
        }
    }
}
