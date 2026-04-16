package zincflow.providers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zincflow.core.ComponentState;
import zincflow.core.Provider;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/// Shells out to the system {@code git} binary so the worker can
/// commit + push its own {@code config.yaml} after a UI-driven edit.
/// JGit intentionally not embedded — ops teams keep control of auth /
/// hooks / refresh-token rotation through their existing git config.
///
/// Enable path (config.yaml):
/// <pre>
/// vc:
///   enabled: true
///   repo: /etc/zincflow        # working tree, defaults to cwd
///   git: /usr/bin/git          # executable, defaults to "git" on PATH
///   remote: origin             # default remote for push
///   branch: main               # default branch for push
/// </pre>
///
/// Operations:
///   - {@link #status()}   — clean flag + ahead/behind + current branch
///   - {@link #commit(String, String)} — stage + commit one path
///   - {@link #push()}     — push to configured remote / branch
///
/// All commands run with a short timeout and stderr captured. Results
/// are surfaced as records so the admin API can echo them as JSON.
public final class VersionControlProvider implements Provider {

    private static final Logger log = LoggerFactory.getLogger(VersionControlProvider.class);

    public static final String NAME = "version_control";

    public record CommandResult(boolean ok, int exitCode, String stdout, String stderr) {
        public CommandResult {
            stdout = stdout == null ? "" : stdout;
            stderr = stderr == null ? "" : stderr;
        }
    }

    public record Status(boolean enabled, boolean clean, int ahead, int behind,
                         String branch, String error) {}

    private final Path repo;
    private final String gitBinary;
    private final String remote;
    private final String branch;
    private volatile ComponentState state = ComponentState.DISABLED;

    public VersionControlProvider(Path repo, String gitBinary, String remote, String branch) {
        this.repo = repo == null ? Path.of(".") : repo;
        this.gitBinary = gitBinary == null || gitBinary.isEmpty() ? "git" : gitBinary;
        this.remote = remote == null || remote.isEmpty() ? "origin" : remote;
        this.branch = branch == null || branch.isEmpty() ? "main" : branch;
    }

    @Override public String name() { return NAME; }
    @Override public String providerType() { return "version_control"; }
    @Override public ComponentState state() { return state; }
    @Override public void enable() { state = ComponentState.ENABLED; }
    @Override public void disable(int drainTimeoutSeconds) { state = ComponentState.DISABLED; }
    @Override public void shutdown() { state = ComponentState.DISABLED; }

    public Path repo() { return repo; }
    public String gitBinary() { return gitBinary; }
    public String remote() { return remote; }
    public String branch() { return branch; }

    public Status status() {
        if (!isEnabled()) return new Status(false, false, 0, 0, "", "provider disabled");
        try {
            CommandResult branchRes = run(List.of(gitBinary, "rev-parse", "--abbrev-ref", "HEAD"));
            String currentBranch = branchRes.ok() ? branchRes.stdout().trim() : branch;

            CommandResult dirty = run(List.of(gitBinary, "status", "--porcelain"));
            boolean clean = dirty.ok() && dirty.stdout().trim().isEmpty();

            int ahead = 0, behind = 0;
            CommandResult revList = run(List.of(
                    gitBinary, "rev-list", "--left-right", "--count",
                    "HEAD..." + remote + "/" + currentBranch));
            if (revList.ok()) {
                String[] parts = revList.stdout().trim().split("\\s+");
                if (parts.length >= 2) {
                    try {
                        ahead = Integer.parseInt(parts[0]);
                        behind = Integer.parseInt(parts[1]);
                    } catch (NumberFormatException ignored) { /* leave at 0 */ }
                }
            }
            return new Status(true, clean, ahead, behind, currentBranch, "");
        } catch (Exception ex) {
            return new Status(true, false, 0, 0, "", ex.toString());
        }
    }

    public CommandResult commit(String relPath, String message) {
        if (!isEnabled()) return new CommandResult(false, -1, "", "provider disabled");
        if (message == null || message.isEmpty()) {
            return new CommandResult(false, -1, "", "commit message must not be blank");
        }
        try {
            CommandResult add = run(List.of(gitBinary, "add", relPath == null ? "." : relPath));
            if (!add.ok()) return add;
            return run(List.of(gitBinary, "commit", "-m", message));
        } catch (Exception ex) {
            return new CommandResult(false, -1, "", ex.toString());
        }
    }

    public CommandResult push() {
        if (!isEnabled()) return new CommandResult(false, -1, "", "provider disabled");
        try { return run(List.of(gitBinary, "push", remote, branch)); }
        catch (Exception ex) { return new CommandResult(false, -1, "", ex.toString()); }
    }

    /// Public for tests — lets the test fixture run arbitrary git
    /// commands against the configured repo + binary.
    public CommandResult run(List<String> command) throws IOException, InterruptedException {
        ProcessBuilder pb = new ProcessBuilder(command).directory(repo.toFile()).redirectErrorStream(false);
        Process proc = pb.start();
        StringBuilder out = new StringBuilder();
        StringBuilder err = new StringBuilder();
        Thread outReader = streamInto(proc.getInputStream(), out);
        Thread errReader = streamInto(proc.getErrorStream(), err);
        boolean done = proc.waitFor(15, TimeUnit.SECONDS);
        if (!done) {
            proc.destroyForcibly();
            return new CommandResult(false, -1, out.toString(), "timeout");
        }
        outReader.join(1000);
        errReader.join(1000);
        int exit = proc.exitValue();
        boolean ok = exit == 0;
        if (!ok) log.warn("git {} failed (exit {}): {}", command, exit, err.toString().trim());
        return new CommandResult(ok, exit, out.toString(), err.toString());
    }

    private static Thread streamInto(java.io.InputStream in, StringBuilder sink) {
        // Virtual thread — drains an external process's stdout/stderr.
        // Blocking reads are a textbook fit for Loom and this pattern
        // fires twice per git invocation, so avoiding platform-thread
        // overhead matters under churn.
        return Thread.ofVirtual().name("zinc-flow-git-stream").start(() -> {
            try (var reader = new java.io.BufferedReader(new java.io.InputStreamReader(in))) {
                String line;
                while ((line = reader.readLine()) != null) sink.append(line).append('\n');
            } catch (IOException ignored) { /* best effort */ }
        });
    }

    public Map<String, Object> statusJson() {
        Status s = status();
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("enabled", s.enabled());
        out.put("clean", s.clean());
        out.put("ahead", s.ahead());
        out.put("behind", s.behind());
        out.put("branch", s.branch());
        out.put("remote", remote);
        out.put("repo", repo.toString());
        if (!s.error().isEmpty()) out.put("error", s.error());
        return out;
    }
}
