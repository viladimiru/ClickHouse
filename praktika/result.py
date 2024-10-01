import dataclasses
import datetime
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

from praktika._environment import _Environment
from praktika._settings import _Settings
from praktika.utils import MetaClasses, Utils, ContextManager, Shell


@dataclasses.dataclass
class Result(MetaClasses.Serializable):
    class Status:
        SKIPPED = "skipped"
        SUCCESS = "success"
        FAILED = "failure"
        PENDING = "pending"
        RUNNING = "running"
        ERROR = "error"

    name: str
    status: str
    start_time: Optional[float] = None
    duration: Optional[float] = None
    results: List["Result"] = dataclasses.field(default_factory=list)
    files: List[str] = dataclasses.field(default_factory=list)
    links: List[str] = dataclasses.field(default_factory=list)
    info: str = ""
    aux_links: List[str] = dataclasses.field(default_factory=list)
    html_link: str = ""

    @staticmethod
    def create_from(
            name="",
            results: List["Result"] = None,
            stopwatch: Utils.Stopwatch = None,
            status="",
            files=None,
    ):
        if isinstance(status, bool):
            status = Result.Status.SUCCESS if status else Result.Status.FAILED
        if not results and not status:
            print("ERROR: Either .results or .status must be provided")
            raise
        if not name:
            name = _Environment.get().JOB_NAME
            if not name:
                print("ERROR: Failed to guess the .name")
                raise
        result_status = status or Result.Status.SUCCESS
        infos = []
        if results and not status:
            for result in results:
                if result.status not in (Result.Status.SUCCESS, Result.Status.FAILED):
                    Utils.raise_with_error(f"Unexpected result status [{result.status}] for Result.create_from call")
                if result.status != Result.Status.SUCCESS:
                    result_status = Result.Status.FAILED
        if results:
            for result in results:
                if result.info:
                    infos.append(f"{result.name}: {result.info}")
        return Result(
            name=name,
            status=result_status,
            start_time=stopwatch.start_time if stopwatch else None,
            duration=stopwatch.duration if stopwatch else None,
            info="\n".join(infos) if infos else "",
            results=results or [],
            files=files or []
        )

    @staticmethod
    def get():
        return Result.from_fs(_Environment.get().JOB_NAME)

    def is_completed(self):
        return self.status not in (Result.Status.PENDING, Result.Status.RUNNING)

    def is_ok(self):
        return self.status in (Result.Status.SKIPPED, Result.Status.SUCCESS)

    def set_status(self, status) -> "Result":
        self.status = status
        self.dump()
        return self

    def set_success(self) -> "Result":
        return self.set_status(Result.Status.SUCCESS)

    def set_results(self, results: List["Result"]) -> "Result":
        self.results = results
        self.dump()
        return self

    def set_files(self, files) -> "Result":
        for file in files:
            assert Path(
                file
            ).is_file(), f"Not valid file [{file}] from file list [{files}]"
        if not self.files:
            self.files = []
        self.files += files
        self.dump()
        return self

    def set_info(self, info: str) -> "Result":
        if self.info:
            self.info += "\n"
        self.info += info
        self.dump()
        return self

    def set_link(self, link) -> "Result":
        self.links.append(link)
        self.dump()
        return self

    @classmethod
    def file_name_static(cls, name):
        return f"{_Settings.TEMP_DIR}/result_{Utils.normalize_string(name)}.json"

    @classmethod
    def from_dict(cls, obj: Dict[str, Any]) -> "Result":
        sub_results = []
        for result_dict in obj["results"] or []:
            sub_res = cls.from_dict(result_dict)
            sub_results.append(sub_res)
        obj["results"] = sub_results
        return Result(**obj)

    def update_duration(self):
        if not self.duration and self.start_time:
            self.duration = datetime.datetime.utcnow().timestamp() - self.start_time
        else:
            if not self.duration:
                print(
                    f"NOTE: duration is set for job [{self.name}] Result - do not update by CI"
                )
            else:
                print(
                    f"NOTE: start_time is not set for job [{self.name}] Result - do not update duration"
                )
        return self

    def update_sub_result(self, result: "Result"):
        assert self.results, "BUG?"
        for i, result_ in enumerate(self.results):
            if result_.name == result.name:
                self.results[i] = result
        self._update_status()
        return self

    def _update_status(self):
        was_pending = False
        was_running = False
        if self.status == self.Status.PENDING:
            was_pending = True
        if self.status == self.Status.RUNNING:
            was_running = True

        has_pending, has_running, has_failed = False, False, False
        for result_ in self.results:
            if result_.status in (self.Status.RUNNING,):
                has_running = True
            if result_.status in (self.Status.PENDING,):
                has_pending = True
            if result_.status in (self.Status.ERROR, self.Status.FAILED):
                has_failed = True
        if has_running:
            self.status = self.Status.RUNNING
        elif has_pending:
            self.status = self.Status.PENDING
        elif has_failed:
            self.status = self.Status.FAILED
        else:
            self.status = self.Status.SUCCESS
        if (was_pending or was_running) and self.status not in (
                self.Status.PENDING,
                self.Status.RUNNING,
        ):
            print("Pipeline finished")
            self.update_duration()

    @classmethod
    def generate_pending(cls, name, results=None):
        return Result(
            name=name,
            status=Result.Status.PENDING,
            start_time=None,
            duration=None,
            results=results or [],
            files=[],
            links=[],
            info="",
        )

    @classmethod
    def generate_skipped(cls, name, results=None):
        return Result(
            name=name,
            status=Result.Status.SKIPPED,
            start_time=None,
            duration=None,
            results=results or [],
            files=[],
            links=[],
            info="from cache",
        )

    @classmethod
    def create_from_command_execution(cls, name, command, workdir=None, with_log=False, fail_fast=True, args=None):
        stop_watch_ = Utils.Stopwatch()
        log_file = f"{_Settings.TEMP_DIR}/{Utils.normalize_string(name)}.log" if with_log else None

        if not isinstance(command, list):
            command = [command]

        print(f"> Start progress for Result [{name}]")
        res = True
        for command_ in command:
            if callable(command_):
                args = args or []
                res = command_(*args)
                if not isinstance(res, bool):
                    print(f"ERROR: Command [{command_}] passed to Result.create_from_command_execution must return bool, got [{res}]")
                    raise
            else:
                with ContextManager.cd(workdir):
                    exit_code = Shell.run(
                        command_,
                        verbose=True,
                        log_file=log_file
                    )
                    res = exit_code == 0
            if not res and fail_fast:
                break
        return Result.create_from(name=name, status=res, stopwatch=stop_watch_, files=[log_file] if log_file else None)

    def finish_job_accordingly(self):
        if not self.is_ok():
            print("ERROR: Job Failed")
            for result in self.results:
                if not result.is_ok():
                    print("Failed checks:")
                    print("  |  ", result)
            sys.exit(1)
        else:
            print("ok")


class ResultInfo:
    SETUP_ENV_JOB_FAILED = (
        "Failed to set up job env, it's praktika bug or misconfiguration"
    )
    PRE_JOB_FAILED = (
        "Failed to do a job pre-run step, it's praktika bug or misconfiguration"
    )
    KILLED = "Job killed or terminated, no Result provided"
    NOT_FOUND_IMPOSSIBLE = (
        "No Result file (bug, or job misbehaviour, must not ever happen)"
    )
    SKIPPED_DUE_TO_PREVIOUS_FAILURE = "Skipped due to previous failure"
    TIMEOUT = "Timeout"

    GH_STATUS_ERROR = "Failed to set GH commit status"

    NOT_FINALIZED = (
        "Job did not not provide Result: job script bug, died CI runner or praktika bug"
    )

    S3_ERROR = "S3 call failure"

    INVALID_RESULT_STATUS = "Invalid Result Status - switched to ERROR"
