from praktika.result import Result
from praktika.settings import Settings
from praktika.utils import Shell, Utils


class ClickHouseProc:
    def __init__(self):
        self.ch_dir = f"{Settings.TEMP_DIR}/db_test"
        self.pid_file = f"{self.ch_dir}/clickhouse-server.pid"
        self.config_file = f"{self.ch_dir}/config.xml"
        self.user_files_path = f"{self.ch_dir}/user_files"
        self.command = f"clickhouse-server --config-file {self.config_file} --pid-file {self.pid_file} -- --path {self.ch_dir} --user_files_path {self.user_files_path} --top_level_domains_path {self.ch_dir}/top_level_domains --keeper_server.storage_path {self.ch_dir}/coordination"
        self.proc = None
        self.pid = 0
        nproc = int(Utils.cpu_count() / 2)
        self.fast_test_command = f"clickhouse-test --hung-check --fast-tests-only --no-random-settings --no-random-merge-tree-settings --no-long --testname --shard --zookeeper --check-zookeeper-session --order random --print-time --report-logs-stats --jobs {nproc} -- ''"
        # TODO: store info in case of failure
        self.info = ""
        self.info_file = ""

        Utils.set_env("CLICKHOUSE_CONFIG_DIR", self.ch_dir)
        Utils.set_env("CLICKHOUSE_CONFIG", self.config_file)
        Utils.set_env("CLICKHOUSE_USER_FILES", self.user_files_path)
        Utils.set_env("CLICKHOUSE_SCHEMA_FILES", f"{self.ch_dir}/format_schemas")

    def start(self):
        print("Starting ClickHouse server")
        Shell.check(f"rm {self.pid_file}")

        self.proc = Shell.run_async(self.command, verbose=True)

        started = True
        try:
            for _ in range(5):
                pid = Shell.get_output(f"cat {self.pid_file}").strip()
                if not pid:
                    Utils.sleep(1)
                    continue
                print(f"Got pid from fs [{pid}]")
                _ = int(pid)
                break
        except Exception:
            started = False

        if not started:
            stdout = self.proc.stdout.read().strip() if self.proc.stdout else ""
            stderr = self.proc.stderr.read().strip() if self.proc.stderr else ""
            Utils.print_formatted_error("Failed to start ClickHouse", stdout, stderr)
            return False

        print(f"ClickHouse server started successfully, pid [{pid}]")
        return True

    def wait_ready(self):
        res, out, err = 0, "", ""
        attempts = 30
        delay = 2
        for attempt in range(attempts):
            res, out, err = Shell.get_res_stdout_stderr(
                'clickhouse-client --query "select 1"', verbose=True
            )
            if out.strip() == "1":
                print("Server ready")
                break
            else:
                print(f"Server not ready, wait")
            Utils.sleep(delay)
        else:
            Utils.print_formatted_error(
                f"Server not ready after [{attempts*delay}s]", out, err
            )
            return False
        return True

    def run_fast_test(self):
        exit_code = Shell.run(self.fast_test_command, dry_run=True)
        return exit_code == 0

    def terminate(self):
        print("Terminate ClickHouse process")
        timeout = 10
        if self.proc:
            Utils.terminate_process_group(self.proc.pid)

            self.proc.terminate()
            try:
                self.proc.wait(timeout=10)
                print(f"Process {self.proc.pid} terminated gracefully.")
            except Exception:
                print(
                    f"Process {self.proc.pid} did not terminate in {timeout} seconds, killing it..."
                )
                Utils.terminate_process_group(self.proc.pid, force=True)
                self.proc.wait()  # Wait for the process to be fully killed
                print(f"Process {self.proc} was killed.")


def clone_submodules():
    submodules_to_update = [
        "contrib/sysroot",
        "contrib/magic_enum",
        "contrib/abseil-cpp",
        "contrib/boost",
        "contrib/zlib-ng",
        "contrib/libxml2",
        "contrib/libunwind",
        "contrib/fmtlib",
        "contrib/aklomp-base64",
        "contrib/cctz",
        "contrib/libcpuid",
        "contrib/libdivide",
        "contrib/double-conversion",
        "contrib/llvm-project",
        "contrib/lz4",
        "contrib/zstd",
        "contrib/fastops",
        "contrib/rapidjson",
        "contrib/re2",
        "contrib/sparsehash-c11",
        "contrib/croaring",
        "contrib/miniselect",
        "contrib/xz",
        "contrib/dragonbox",
        "contrib/fast_float",
        "contrib/NuRaft",
        "contrib/jemalloc",
        "contrib/replxx",
        "contrib/wyhash",
        "contrib/c-ares",
        "contrib/morton-nd",
        "contrib/xxHash",
        "contrib/expected",
        "contrib/simdjson",
        "contrib/liburing",
        "contrib/libfiu",
        "contrib/incbin",
        "contrib/yaml-cpp",
    ]

    res = Shell.check("git submodule sync", verbose=True, strict=True)
    res = res and Shell.check("git submodule init", verbose=True, strict=True)
    res = res and Shell.check(
        command=f"xargs --max-procs={Utils.cpu_count()} --null --no-run-if-empty --max-args=1 git submodule update --depth 1 --single-branch",
        stdin_str="\0".join(submodules_to_update) + "\0",
        timeout=60,
        retries=3,
        verbose=True,
    )
    res = res and Shell.check("git submodule foreach git reset --hard", verbose=True)
    res = res and Shell.check("git submodule foreach git checkout @ -f", verbose=True)
    res = res and Shell.check("git submodule foreach git clean -xfd", verbose=True)
    return res


def update_path_ch_config(config_file_path=""):
    print("Updating path in clickhouse config")
    config_file_path = config_file_path or f"{Settings.TEMP_DIR}/db_test/config.xml"
    try:
        with open(config_file_path, "r", encoding="utf-8") as file:
            content = file.read()

        content = content.replace("/var/", f"{Settings.TEMP_DIR}/var/")
        with open(config_file_path, "w", encoding="utf-8") as file:
            file.write(content)
    except Exception as e:
        print(f"ERROR: failed to update config, exception: {e}")
        return False
    return True


def main():
    stop_watch = Utils.Stopwatch()

    res = True

    current_directory = Utils.cwd()
    build_dir = f"{Settings.TEMP_DIR}/build"

    Shell.check("sccache --show-stats")
    # Shell.check(f"rm -rf {build_dir} && mkdir -p {build_dir}")
    results = []

    # if res:
    #     results.append(
    #         Result.create_from_command_execution(
    #             name="Checkout Submodules for Minimal Build",
    #             command=clone_submodules,
    #         )
    #     )
    #     res = results[-1].is_ok()
    #
    # if res:
    #     results.append(
    #         Result.create_from_command_execution(
    #             name="Cmake configuration",
    #             command=f"cmake {current_directory} -DCMAKE_CXX_COMPILER=clang++-18 -DCMAKE_C_COMPILER=clang-18 \
    #             -DCMAKE_TOOLCHAIN_FILE={current_directory}/cmake/linux/toolchain-x86_64-musl.cmake -DENABLE_LIBRARIES=0 \
    #             -DENABLE_TESTS=0 -DENABLE_UTILS=0 -DENABLE_THINLTO=0 -DENABLE_NURAFT=1 -DENABLE_SIMDJSON=1 \
    #             -DENABLE_JEMALLOC=1 -DENABLE_LIBURING=1 -DENABLE_YAML_CPP=1 -DCOMPILER_CACHE=sccache",
    #             workdir=build_dir,
    #             with_log=True,
    #         )
    #     )
    #     res = results[-1].is_ok()
    #
    # if res:
    #     results.append(
    #         Result.create_from_command_execution(
    #             name="Build ClickHouse",
    #             command="ninja clickhouse-bundle clickhouse-stripped",
    #             workdir=build_dir,
    #             with_log=True,
    #         )
    #     )
    #     res = results[-1].is_ok()

    if res:
        Utils.add_to_PATH(f"{build_dir}/programs:{current_directory}/tests")
        commands = [
            f"mkdir -p {Settings.OUTPUT_DIR}/binaries",
            f"cp ./programs/clickhouse {Settings.OUTPUT_DIR}/binaries/clickhouse",
            f"zstd --threads=0 --force programs/clickhouse-stripped -o {Settings.OUTPUT_DIR}/binaries/clickhouse-stripped.zst",
            "sccache --show-stats",
            "clickhouse-client --version",
            "clickhouse-test --help",
        ]
        results.append(
            Result.create_from_command_execution(
                name="Check and Compress binary",
                command=commands,
                workdir=build_dir,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    if res:
        commands = [
            f"rm -rf {Settings.TEMP_DIR}/db_test && mkdir -p {Settings.TEMP_DIR}/db_test/client-config",
            f"cp {current_directory}/programs/server/config.xml {current_directory}/programs/server/users.xml {Settings.TEMP_DIR}/db_test/",
            f"cp -a {current_directory}/programs/server/config.d/log_to_console.xml {Settings.TEMP_DIR}/db_test/config.d",
            f"cp -a {current_directory}/programs/server/config.d/log_to_console.xml {Settings.TEMP_DIR}/db_test/config.d",
            f"rm -f {Settings.TEMP_DIR}/db_test/config.d/secure_ports.xml",
            update_path_ch_config,
        ]
        results.append(
            Result.create_from_command_execution(
                name="Install ClickHouse Config",
                command=commands,
                with_log=True,
            )
        )
        res = results[-1].is_ok()

    CH = ClickHouseProc()
    if res:
        stop_watch_ = Utils.Stopwatch()
        step_name = "Start ClickHouse Server"
        print(step_name)
        res = CH.start()
        res = res and CH.wait_ready()
        res = res and CH.run_fast_test()
        Utils.sleep(600)
        results.append(
            Result.create_from(name=step_name, status=res, stopwatch=stop_watch_)
        )

    CH.terminate()

    Result.create_from(
        results=results, stopwatch=stop_watch
    ).dump().finish_job_accordingly()


if __name__ == "__main__":
    main()
