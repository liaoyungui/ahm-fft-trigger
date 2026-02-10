// FFTTriggerController.cs
//
// AHM FFT Trigger Controller (v2.2.1) — RULE ONLY / HARDENED
//
// Key hardening vs v2.2.0:
// - Avoids IParametersAnReSa.writeToDevice() (can native-abort in libVSEUtilities.so)
// - Triggers only on new status_row_ts (no spam when operator_status.json updates frequently)
// - Persists controller state to disk (survive restart without retriggering same row)
// - Keeps workflow deterministic/offline-first
//
// Workflow on operator_zone ENTERS C/D:
//   1) lock (single instance)
//   2) stop RMS poller service (exclusive VSE access)
//   3) pre-FFT quick RMS window (raw stream -> RMS)
//   4) raw capture for C1/C2 (+C3 if enabled)
//   5) post-FFT quick RMS window
//   6) write event marker JSON
//   7) start RMS poller service
//
// Build (Mono):
//   mcs -langversion:latest -out:build/fft_trigger_controller.exe \
//       -r:../VSEUtilities_API.dll FFTTriggerController.cs
//
// Run:
//   export MONO_PATH=/home/sdc/yg/work/chiller/FFT/MyVSEProject
//   export LD_LIBRARY_PATH=/home/sdc/yg/work/chiller/FFT/VSEUtilities_Linux/build-VSEUtilities_Linux/src/library:$LD_LIBRARY_PATH
//   mono build/fft_trigger_controller.exe

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
    private const string VERSION = "2.2.2";

    // =========================
    // VSE CONFIG (match RMS Poller v2.0.6)
    // =========================
    private const string IpVse1 = "192.168.1.101";
    private const short PortVse1 = 3321;

    private const bool EnableVse2 = true;   // set true for 3-compressor chillers
    private const string IpVse2 = "192.168.1.102";
    private const short PortVse2 = 3321;

    // =========================
    // INPUT: Operator Zone Engine v2.1.0
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
    private static readonly string StateFile = Path.Combine(RuntimeRoot, "controller_state.json");
    private static readonly string LockFile = Path.Combine(LockDir, "vse_exclusive.lock");

    // =========================
    // RMS POLLER SERVICE (exclusive control)
    // =========================
    private const string RmsPollerServiceName = "ahm-rms-poller.service";

    // =========================
    // TRIGGER RULES
    // =========================
    private static readonly HashSet<string> TriggerZones =
        new HashSet<string>(StringComparer.OrdinalIgnoreCase) { "C", "D" };

    private const int CooldownSeconds = 10 * 60;
    private const int PollIntervalMs = 1000;

    // =========================
    // PRE/POST quick read parameters
    // =========================
    private const int PrePostWindowMs = 2000;   // 2s sampling window
    private const double RAW_TO_MMPS = 1.0;     // keep consistent with your existing RMS assumptions
    private const string OverallMode = "MAX_AXIS"; // "RSS" or "MAX_AXIS"

    // =========================
    // FFT RAW CAPTURE parameters
    // =========================
    private const int FftSampleRate = 100000;   // informational
    private const int FftSampleSize = 131072;   // required samples per axis
    private const int FftTimeoutSeconds = 20;

    
// =========================
// START/STOP HARDENING
// =========================
private const int StartRetryCount = 3;
private const int StartRetryDelayMs = 600;
private const int AfterStopSettleMs = 600;
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
    private static string _lastSeenStatusRowTs = null;     // in-memory
    private static string _lastProcessedStatusRowTs = null; // persisted

    public static int Main(string[] args)
    {
        Console.WriteLine($"=== AHM FFT Trigger Controller v{VERSION} ===");
        Console.WriteLine($"OperatorStatusJson : {OperatorStatusJson}");
        Console.WriteLine($"RuntimeRoot        : {RuntimeRoot}");
        Console.WriteLine($"EnableVse2         : {EnableVse2}");
        Console.WriteLine($"RmsPollerService   : {RmsPollerServiceName}");
        Console.WriteLine($"FFT sampleSize     : {FftSampleSize}, fs={FftSampleRate}");
        Console.WriteLine($"StateFile          : {StateFile}");

        EnsureDirs();
        LoadControllerState();

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

                // Only act on NEW status row timestamp (prevents spam)
                if (!string.IsNullOrWhiteSpace(status.StatusRowTs))
                {
                    if (_lastSeenStatusRowTs == status.StatusRowTs)
                    {
                        Thread.Sleep(PollIntervalMs);
                        continue;
                    }
                    _lastSeenStatusRowTs = status.StatusRowTs;

                    // If we already processed this exact row before restart, ignore
                    if (_lastProcessedStatusRowTs == status.StatusRowTs)
                    {
                        Thread.Sleep(PollIntervalMs);
                        continue;
                    }
                }

                string opZone = (status.OperatorZone ?? "A").Trim().ToUpperInvariant();
                string instantZone = (status.InstantZone ?? "A").Trim().ToUpperInvariant();

                bool inCooldown = _lastTriggerUtc != DateTime.MinValue &&
                                  (DateTime.UtcNow - _lastTriggerUtc).TotalSeconds < CooldownSeconds;

                bool enteringTrigger = TriggerZones.Contains(opZone) && !TriggerZones.Contains(_lastOperatorZone);

                if (enteringTrigger && !inCooldown)
                {
                    Console.WriteLine($"[TRIGGER] operator_zone entered {opZone} (from {_lastOperatorZone}).");
                    RunFftWorkflow(status, opZone, instantZone);
                    _lastTriggerUtc = DateTime.UtcNow;

                    // mark this status_row_ts as processed (persist)
                    if (!string.IsNullOrWhiteSpace(status.StatusRowTs))
                    {
                        _lastProcessedStatusRowTs = status.StatusRowTs;
                        SaveControllerState();
                    }
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
            lockHandle = AcquireExclusiveLock();

            // stop RMS poller for exclusive VSE access
            Systemctl("stop", RmsPollerServiceName);

// Wait until RMS poller is really inactive (avoid VSE handle still held)
WaitServiceInactive(RmsPollerServiceName, 8000);

// Ensure THIS controller has a clean slate too (no stale raw streams / sockets)
SafeStopAll();
Thread.Sleep(AfterStopSettleMs);
            var pre = QuickOverallRead();

            var fftFiles = new List<string>();
            if (CaptureFftVse1(dayDir, eventId, out var c1, out var c2))
            {
                if (!string.IsNullOrEmpty(c1)) fftFiles.Add(c1);
                if (!string.IsNullOrEmpty(c2)) fftFiles.Add(c2);
            }

            if (EnableVse2)
            {
                // VSE2 tends to need extra settle time between start/stop cycles
                Thread.Sleep(1500);

                if (CaptureFftVse2(dayDir, eventId, out var c3))
                {
                    if (!string.IsNullOrEmpty(c3)) fftFiles.Add(c3);
                }
            }

            // 5) Allow VSE firmware to settle after FFT
            Thread.Sleep(2000);

            (double Overall, object C1, object C2, object C3) post;
            try
            {
                post = QuickOverallRead();
            }
            catch (Exception ex)
            {
                Console.WriteLine("[WARN] Post-FFT QuickOverallRead failed: " + ex.Message);
                post = (0, null, null, null);
            }


            File.WriteAllText(eventJsonPath, BuildEventJson(eventId, opZone, instantZone, status.StatusRowTs, pre, post, fftFiles));
            Console.WriteLine("[EVENT] " + eventJsonPath);
        }
        catch (Exception ex)
        {
            Console.WriteLine("[WorkflowError] " + ex);
        }
        finally
        {
            // always try to restart RMS poller
            try { Systemctl("start", RmsPollerServiceName); } catch { }

            SafeStopAll();
            try { lockHandle?.Dispose(); } catch { }
        }
    }

    private static string BuildEventJson(
        string eventId,
        string opZone,
        string instantZone,
        string statusRowTs,
        (double Overall, object C1, object C2, object C3) pre,
        (double Overall, object C1, object C2, object C3) post,
        List<string> fftFiles)
    {
        var sb = new StringBuilder(2048);
        sb.AppendLine("{");
        sb.AppendLine($"  \"ts\": \"{NowIsoLocal()}\",");
        sb.AppendLine($"  \"version\": \"{VERSION}\",");
        sb.AppendLine($"  \"event_id\": \"{eventId}\",");
        sb.AppendLine($"  \"operator_zone\": \"{opZone}\",");
        sb.AppendLine($"  \"instant_zone\": \"{instantZone}\",");
        sb.AppendLine($"  \"status_row_ts\": \"{Escape(statusRowTs)}\",");
        sb.AppendLine($"  \"pre_overall_mmps\": {pre.Overall.ToString("G10", CultureInfo.InvariantCulture)},");
        sb.AppendLine($"  \"post_overall_mmps\": {post.Overall.ToString("G10", CultureInfo.InvariantCulture)},");
        sb.AppendLine("  \"fft_files\": [");
        for (int i = 0; i < fftFiles.Count; i++)
        {
            sb.Append("    \"").Append(Escape(fftFiles[i])).Append("\"");
            if (i < fftFiles.Count - 1) sb.Append(",");
            sb.AppendLine();
        }
        sb.AppendLine("  ],");
        sb.AppendLine("  \"source\": \"ahm_local\"");
        sb.AppendLine("}");
        return sb.ToString();
    }

    // =========================================================
    // QUICK OVERALL READ (pre/post) — NO writeToDevice()
    // =========================================================
    private static (double Overall, object C1, object C2, object C3) QuickOverallRead()
    {
        EnsureVseConnected();

        var untilUtc = DateTime.UtcNow.AddMilliseconds(PrePostWindowMs);

        var bufC1 = new List<(float x, float z)>();
        var bufC2 = new List<(float x, float z)>();
        var bufC3 = new List<(float x, float z)>();

        // VSE1 collector: 0..3 => C1(X,Z), C2(X,Z)
        var col1 = new RawCollector(
            onVse1: (s1, s2, s3, s4) =>
            {
                bufC1.Add((s1, s2));
                bufC2.Add((s3, s4));
            },
            onVse2: null);

        _rawVse1 = new IRawDataAnReSa();
bool ok1 = StartRawWithRetry(
    "[QuickOverallRead] VSE1",
    startFunc: () => _rawVse1.start(_vse1, col1, null, null),
    stopFunc: () => { try { _rawVse1.stop(); } catch { } },
    reconnectFunc: () => { SafeStopAll(); EnsureVseConnected(); }
);
if (!ok1) throw new Exception("QuickOverallRead VSE1 start failed after retries.");
        // VSE2 collector: 0..1 => C3(X,Z)
        if (EnableVse2 && _vse2 != null)
        {
            Thread.Sleep(300); // small settle before opening VSE2 stream

            var col2 = new RawCollector(
                onVse1: null,
                onVse2: (s1, s2) =>
                {
                    bufC3.Add((s1, s2));
                });

            _rawVse2 = new IRawDataAnReSa();
bool ok2 = StartRawWithRetry(
    "[QuickOverallRead] VSE2",
    startFunc: () => _rawVse2.start(_vse2, col2, null, null),
    stopFunc: () => { try { _rawVse2.stop(); } catch { } },
    reconnectFunc: () => { try { _vse2?.disconnect(); } catch { } _vse2 = null; EnsureVseConnected(); }
);
if (!ok2)
{
    Console.WriteLine("[INFO] Post-FFT QuickOverallRead VSE2 skipped (device settling)");
    try { _rawVse2.stop(); } catch { }
        _rawVse2 = null;
        Thread.Sleep(AfterStopSettleMs);

    _rawVse2 = null;
}
        }

        while (DateTime.UtcNow < untilUtc)
            Thread.Sleep(20);

        try { _rawVse1?.stop(); } catch { }
        _rawVse1 = null;
        try { _rawVse2?.stop(); } catch { }
        _rawVse2 = null;
        Thread.Sleep(AfterStopSettleMs);

        var c1 = ComputeRms(bufC1);
        var c2 = ComputeRms(bufC2);
        var c3 = ComputeRms(bufC3);

        double overall = EnableVse2
            ? Math.Max(c1.overall, Math.Max(c2.overall, c3.overall))
            : Math.Max(c1.overall, c2.overall);

        object c1o = new { rms_x = c1.rmsX, rms_z = c1.rmsZ, overall = c1.overall, n = c1.n };
        object c2o = new { rms_x = c2.rmsX, rms_z = c2.rmsZ, overall = c2.overall, n = c2.n };
        object c3o = new { rms_x = c3.rmsX, rms_z = c3.rmsZ, overall = c3.overall, n = c3.n };

        return (overall, c1o, c2o, c3o);
    }

    private static (double rmsX, double rmsZ, double overall, int n) ComputeRms(List<(float x, float z)> buf)
    {
        if (buf == null || buf.Count < 5)
            return (0, 0, 0, buf?.Count ?? 0);

        double[] xs = buf.Select(p => (double)p.x).ToArray();
        double[] zs = buf.Select(p => (double)p.z).ToArray();

        double rmsX = Math.Sqrt(xs.Select(v => v * v).Average()) * RAW_TO_MMPS;
        double rmsZ = Math.Sqrt(zs.Select(v => v * v).Average()) * RAW_TO_MMPS;

        double overall = (OverallMode == "RSS")
            ? Math.Sqrt(rmsX * rmsX + rmsZ * rmsZ)
            : Math.Max(rmsX, rmsZ);

        return (rmsX, rmsZ, overall, buf.Count);
    }

    // =========================================================
    // FFT RAW CAPTURE — NO writeToDevice()
    // =========================================================
    private static bool CaptureFftVse1(string dayDir, string eventId, out string fileC1, out string fileC2)
    {
        fileC1 = null;
        fileC2 = null;

        EnsureVseConnected();

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
bool ok = StartRawWithRetry(
    "[FFT] VSE1",
    startFunc: () => _rawVse1.start(_vse1, col, null, null),
    stopFunc: () => { try { _rawVse1.stop(); } catch { } },
    reconnectFunc: () => { SafeStopAll(); EnsureVseConnected(); }
);
if (!ok)
{
    Console.WriteLine("FFT VSE1 start failed after retries.");
    return false;
}
        var startUtc = DateTime.UtcNow;
        while (!done && (DateTime.UtcNow - startUtc).TotalSeconds < FftTimeoutSeconds)
            Thread.Sleep(5);

        try { _rawVse1.stop(); } catch { }
        _rawVse1 = null;
        Thread.Sleep(AfterStopSettleMs);

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
bool ok = StartRawWithRetry(
    "[FFT] VSE2",
    startFunc: () => _rawVse2.start(_vse2, col, null, null),
    stopFunc: () => { try { _rawVse2.stop(); } catch { } },
    reconnectFunc: () => { try { _vse2?.disconnect(); } catch { } _vse2 = null; EnsureVseConnected(); }
);
if (!ok)
{
    Console.WriteLine("FFT VSE2 start failed after retries.");
    return false;
}
        var startUtc = DateTime.UtcNow;
        while (!done && (DateTime.UtcNow - startUtc).TotalSeconds < FftTimeoutSeconds)
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
    // VSE CONNECTION
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
    // OPERATOR STATUS READER (simple JSON string extract, Mono-friendly)
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
            string srt = ExtractJsonValue(json, "status_row_ts");

            return new OperatorStatus
            {
                OperatorZone = op,
                InstantZone = instant,
                StatusRowTs = srt
            };
        }
        catch
        {
            return null;
        }
    }

    private static string ExtractJsonValue(string json, string key)
    {
        string token = $"\"{key}\"";
        int idx = json.IndexOf(token);
        if (idx < 0) return null;

        int colon = json.IndexOf(':', idx);
        if (colon < 0) return null;

        // find first quote after colon
        int q1 = json.IndexOf('"', colon + 1);
        if (q1 < 0) return null;
        int q2 = json.IndexOf('"', q1 + 1);
        if (q2 < 0) return null;

        return json.Substring(q1 + 1, q2 - (q1 + 1));
    }

    private class OperatorStatus
    {
        public string OperatorZone { get; set; }
        public string InstantZone { get; set; }
        public string StatusRowTs { get; set; }
    }

    // =========================================================
    // PERSISTED CONTROLLER STATE
    // =========================================================
    private static void LoadControllerState()
    {
        try
        {
            if (!File.Exists(StateFile)) return;
            var json = File.ReadAllText(StateFile);
            _lastProcessedStatusRowTs = ExtractJsonValue(json, "last_processed_status_row_ts");
            var lastZone = ExtractJsonValue(json, "last_operator_zone");
            if (!string.IsNullOrWhiteSpace(lastZone)) _lastOperatorZone = lastZone.Trim().ToUpperInvariant();
            Console.WriteLine($"[STATE] last_processed_status_row_ts={_lastProcessedStatusRowTs}, last_operator_zone={_lastOperatorZone}");
        }
        catch { }
    }

    private static void SaveControllerState()
    {
        try
        {
            var sb = new StringBuilder(256);
            sb.AppendLine("{");
            sb.AppendLine($"  \"ts\": \"{NowIsoLocal()}\",");
            sb.AppendLine($"  \"last_processed_status_row_ts\": \"{Escape(_lastProcessedStatusRowTs)}\",");
            sb.AppendLine($"  \"last_operator_zone\": \"{Escape(_lastOperatorZone)}\"");
            sb.AppendLine("}");
            File.WriteAllText(StateFile, sb.ToString());
        }
        catch { }
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
// SERVICE WAIT / START RETRIES
// =========================================================
private static void WaitServiceInactive(string serviceName, int timeoutMs)
{
    var until = DateTime.UtcNow.AddMilliseconds(timeoutMs);
    while (DateTime.UtcNow < until)
    {
        try
        {
            // systemctl is-active exits 0 when active; non-zero otherwise
            var p = new Process();
            p.StartInfo.FileName = "/bin/systemctl";
            p.StartInfo.Arguments = $"is-active {serviceName}";
            p.StartInfo.RedirectStandardOutput = true;
            p.StartInfo.RedirectStandardError = true;
            p.StartInfo.UseShellExecute = false;
            p.StartInfo.CreateNoWindow = true;
            p.Start();
            string o = p.StandardOutput.ReadToEnd().Trim();
            p.WaitForExit();
            if (!string.Equals(o, "active", StringComparison.OrdinalIgnoreCase) &&
                !string.Equals(o, "activating", StringComparison.OrdinalIgnoreCase))
                return;
        }
        catch
        {
            return; // best effort
        }

        Thread.Sleep(200);
    }
    Console.WriteLine($"[WARN] Timeout waiting service inactive: {serviceName}");
}


private static bool StatusIsOk(object st)
{
    if (st == null) return false;
    try
    {
        var m = st.GetType().GetMethod("isOk", Type.EmptyTypes);
        if (m != null && m.ReturnType == typeof(bool))
            return (bool)m.Invoke(st, null);
    }
    catch { }
    return false;
}

private static string StatusText(object st)
{
    if (st == null) return "(null status)";
    try
    {
        var m = st.GetType().GetMethod("text", Type.EmptyTypes);
        if (m != null)
        {
            var v = m.Invoke(st, null);
            return v?.ToString() ?? "(null text)";
        }
    }
    catch { }
    return st.ToString();
}

private static bool StartRawWithRetry(string tag, Func<object> startFunc, Action stopFunc = null, Action reconnectFunc = null)
{
    for (int attempt = 1; attempt <= StartRetryCount; attempt++)
    {
        try
        {
            var st = startFunc();
            if (StatusIsOk(st))
                return true;

            string msg = StatusText(st);
            Console.WriteLine($"{tag} start failed (attempt {attempt}/{StartRetryCount}): {msg}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"{tag} start exception (attempt {attempt}/{StartRetryCount}): {ex.Message}");
        }

        // stop any partial stream
        try { stopFunc?.Invoke(); } catch { }

        // reconnect VSE socket if provided
        try { reconnectFunc?.Invoke(); } catch { }

        Thread.Sleep(StartRetryDelayMs);
    }
    return false;
}
// =========================================================
    // SYSTEMD CONTROL
    // =========================================================
    private static void Systemctl(string action, string serviceName)
    {
        RunCmd("/bin/systemctl", $"{action} {serviceName}");
        //RunCmd("/usr/bin/sudo", $"-n /bin/systemctl {action} {serviceName}");

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

    private static string Escape(string s)
    {
        if (string.IsNullOrEmpty(s)) return "";
        return s.Replace("\\", "\\\\").Replace("\"", "\\\"");
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