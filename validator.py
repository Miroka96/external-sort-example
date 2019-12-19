#!/usr/bin/env python3
# -*- coding: UTF-8 -*-

"""
BigDataSystems - WT19 - Exercise #1: External Sorting

Build the jar before running this script. This file expects a exsort.jar at build/libs/export.jar.
This can be built with
    > ./gradlew shadowJar

Don't forget to re-build the jar every time you make changes to your java code.

Execute this in the root of the code that we provide (/path/to/exsort/)
Usage: python3 validator.py

Based on code from Sven Koehler <sven.koehler@hpi.de>.
"""

import os
import shutil
import subprocess
import sys
import time

# -------------------------------------------------------------------------------

Test_cwd = None

output_lines = []


def log(msg="", new_line=True, okay=False):
    output_lines.append((okay, "%s\n" % msg if new_line else msg))


# -------------------------------------------------------------------------------

class Test(object):

    def exec_(self, *cmd_and_args):
        p = subprocess.run(cmd_and_args, cwd=Test_cwd,
                           universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return (p.stdout, p.stderr, p.returncode)

    def run_prog(self, *cmd_and_args):
        return self.exec_("", *cmd_and_args)[0]

    def test(self):
        self.run()
        okay = self.okay()
        log(self.what(), okay=okay)
        return okay

    def run(self):
        raise NotImplementedError

    def what(self):
        raise NotImplementedError

    def okay(self):
        raise NotImplementedError


class TestGroup(Test):
    """A group of tests, where all tests are executed"""

    def __init__(self, *tests):
        self.tests = tests
        self.is_okay = False

    def test(self):
        self.is_okay = all([t.test() for t in self.tests])
        return self.is_okay

    def okay(self):
        return self.is_okay


class ReturnCodeTest(Test):

    def __init__(self, progname, args=None, retcode=0, showstdout=False, showstderr=False, inputText="", truncateLines=None):
        self.progname = progname
        self.inputText = inputText
        self.cmdline = [progname] + list(map(str, args or []))
        self.expected = retcode
        self.exception = None
        self.showstdout = showstdout
        self.showstderr = showstderr
        self.output = None
        self.truncateLines = truncateLines
        self.stdout = ""
        self.stderr = ""

    def _truncate(self, text):
        # Hoping for PEP 572 to fix this mess
        #    https://www.python.org/dev/peps/pep-0572/
        # Okay, hoping for submit-exec to support Python 3.8

        lines = text.split("\n")
        if self.truncateLines is None or len(lines) <= self.truncateLines:
            return text

        return "\n".join(lines[:self.truncateLines] + ["", "[OUTPUT TRUNCATED]"])

    def _get_output(self, tpl):
        self.stdout = tpl[0]
        self.stderr = tpl[1]
        return tpl[2]

    def run(self):
        try:
            self.output = self._get_output(self.exec_(*self.cmdline))
        except Exception as e:
            self.exception = e
            self.exc_info = sys.exc_info()

    def _s(self, a):
        return " ".join(map(str, a))

    def _format_call(self):
        if self.inputText:
            return "echo -ne %r | %s" % (self.inputText, self._s(self.cmdline))
        return self._s(self.cmdline)

    def what(self):
        return "$ %s #- exited with %s (expected %d)%s%s" % (
            self._format_call(),
            str(self.output) if self.exception is None else repr(str(self.exception)),
            self.expected,
            "" if not (self.showstdout and self.stdout) else ("\n%s" % self._truncate(self.stdout)),
            "" if not (self.showstderr and self.stderr) else ("\n%s" % self._truncate(self.stderr)),
        )

    def okay(self):
        return self.output == self.expected


class ExecutionTest(ReturnCodeTest):
    """Run a program and compare the lines of stdout to a list of expected output."""

    def __init__(self, prog_name, args, expected, name=None):
        super().__init__(prog_name, args, expected)
        self.name = name

    def _get_output(self, tpl):
        super(ExecutionTest, self)._get_output(tpl)
        return self.stdout.strip()

    def what(self):
        _r = repr
        description = self.name if self.name is not None else f"$ {self._format_call()}"
        return description + "\n" + \
               ("" if not (self.showstderr and self.stderr) else ("%s\n" % self.stderr)) + \
               (("received: " + _r(self.output))
                if self.exception is None
                else str(self.exception)) + "\n" + \
               "expected: " + _r(self.expected) + '\n' + \
               f"--> This test {'passed :)' if self.okay() else 'failed :('}"

    def okay(self):
        return self.exception is None and self.output == self.expected


class MultiProgramTest(ExecutionTest):
    """Start background programs before running the test program."""

    def __init__(self, prog_name, args, expected, background_progs, name=None):
        super().__init__(prog_name, args, expected, name)
        self.background_progs = background_progs

    def run(self):
        # Start background programs
        processes = []
        for prog in self.background_progs:
            print(f"Starting background job: {prog}")
            p = subprocess.Popen(prog, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            processes.append(p)

        # Allow background processes to start
        time.sleep(1)

        try:
            print("Running solution...")
            return super().run()
        finally:
            print("Solution finished.")
            print("Killing background processes...")
            for p in processes:
                p.kill()
                p_stdout, p_stderr = p.communicate()
                print(f"Background job stdout: {p_stdout}")
                print(f"Background job stderr: {p_stderr}")
            print("Killing background processes done.")

# -------------------------------------------------------------------------------


ROOT_DIR = "external-sort-exercise"
RUNNER_IMG = "hpides/base-runner-image"
JAR_PATH = "build/libs/exsort.jar"


def run_cmd(cmd):
    cmd = " ".join([str(x) for x in cmd])
    return subprocess.run(cmd, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)


def gradle_build(job):
    print(f"Building with gradle...")
    build_process = run_cmd(["./gradlew", "shadowJar"])

    if build_process.returncode != 0:
        job.send_fail_result("Gradle cannot build you solution."
                             f"\n\tstdout: {build_process.stdout}"
                             f"\n\tstderr: {build_process.stderr}")
        sys.exit(1)

# -------------------------------------------------------------------------------


def make_local_exsort_test(is_validate):
    expected_msg = "File was sorted correctly."
    java_args = ["-cp", JAR_PATH, "com.github.hpides.exsort.executables.LocalSorterMain"]

    if is_validate:
        # Opensubmit execution
        exec_cmd = "docker"
        current_dir = os.path.abspath('.')
        jar_volume_arg = f"{os.path.join(current_dir, JAR_PATH)}:/{JAR_PATH}"
        data_volume_arg = f"{os.path.join(current_dir, 'data')}:/data"
        docker_args = ["run", "--rm", "-v", jar_volume_arg, "-v", data_volume_arg, RUNNER_IMG]
        exec_args = docker_args + ["java"] + java_args
    else:
        # Local execution
        exec_cmd = "java"
        exec_args = java_args

    def ex_sort_test(name, in_file, out_file, memory_limit, expected_file):
        run_args = [in_file, out_file, memory_limit, expected_file]
        return ExecutionTest(exec_cmd, exec_args + run_args, expected_msg, name=name)

    return ex_sort_test


def make_remote_exsort_test(remotes, is_validate):
    expected_msg = "File was sorted correctly."
    java_args = ["-cp", JAR_PATH, "com.github.hpides.exsort.executables.RemoteSorterMain"]
    server_java_args = ["-cp", JAR_PATH, "com.github.hpides.exsort.executables.RemoteServerMain"]

    if is_validate:
        # Opensubmit execution
        exec_cmd = "docker"
        current_dir = os.path.abspath('.')
        jar_volume_arg = f"{os.path.join(current_dir, JAR_PATH)}:/{JAR_PATH}"
        data_volume_arg = f"{os.path.join(current_dir, 'data')}:/data"
        docker_args = ["run", "--rm", "--network", "host", "-v", jar_volume_arg, "-v", data_volume_arg, RUNNER_IMG]
        exec_args = docker_args + ["java"] + java_args

        server_cmds = []
        for _, port in remotes:
            server_cmd = [exec_cmd, "run", "--rm", "--network", "host", "-v", jar_volume_arg, "-v", data_volume_arg,
                          RUNNER_IMG, "java"] + server_java_args + [str(port)]
            server_cmds.append(server_cmd)
    else:
        # Local execution
        exec_cmd = "java"
        exec_args = java_args
        server_cmds = [[exec_cmd] + server_java_args + [str(port)] for _, port in remotes]

    def ex_sort_test(name, in_file, out_file, memory_limit, expected_file):
        remote_strings = [f"{ip}:{p}" for ip, p in remotes]
        run_args = [in_file, out_file, memory_limit, expected_file] + remote_strings

        return MultiProgramTest(exec_cmd, exec_args + run_args, expected_msg, server_cmds, name=name)

    return ex_sort_test


# -------------------------------------------------------------------------------

def main(is_validate=False):
    global NEXT_PORT
    local_exsort_test = make_local_exsort_test(is_validate)
    local_tests = TestGroup(
        local_exsort_test("small1", "data/unsorted_a.txt", "data/sorted_a_10b.txt",   10, "data/expected_a.txt"),
        local_exsort_test("small2", "data/unsorted_a.txt", "data/sorted_a_100b.txt", 100, "data/expected_a.txt"),
        local_exsort_test("small3", "data/unsorted_a.txt", "data/sorted_a_1kb.txt", 1000, "data/expected_a.txt"),
        local_exsort_test("1MB-100kb", "data/unsorted_1MB.txt", "data/sorted_1MB_100kb.txt", 100_000, "data/expected_1MB.txt"),
        local_exsort_test("10MB-1MB", "data/unsorted_10MB.txt", "data/sorted_10MB_1mb.txt", 1_000_000, "data/expected_10MB.txt"),
    )

    local_is_valid = local_tests.test()

    remotes = [("localhost", 5000), ("localhost", 5001)]
    remote_exsort_test = make_remote_exsort_test(remotes, is_validate)
    remote_tests = TestGroup(
        remote_exsort_test("small_dist", "data/unsorted_a.txt", "data/sorted_a_10b_dist.txt", 10, "data/expected_a_dist.txt"),
        remote_exsort_test("1MB-100kb-dist", "data/unsorted_1MB.txt", "data/sorted_1MB_100kb_dist.txt", 100_000, "data/expected_1MB_dist.txt"),
        remote_exsort_test("10MB-1MB-dist", "data/unsorted_10MB.txt", "data/sorted_10MB_1mb_dist.txt", 1_000_000, "data/expected_10MB_dist.txt"),
    )

    remote_is_valid = remote_tests.test()
    return local_is_valid and remote_is_valid


# -------------------------------------------------------------------------------

def validate(job):
    global Test_cwd
    Test_cwd = job.working_dir

    if not job.ensure_files(["RemoteFileSorter.java", "LocalFileSorter.java"]):
        return job.send_fail_result("You need to upload RemoteFileSorter.java and LocalFileSorter.java")

    # Copy zip to working dir
    shutil.copyfile("/home/opensubmit/external-sort-exercise.zip", os.path.join(Test_cwd, "exsort.zip"))
    os.chdir(Test_cwd)
    run_cmd(["unzip", "exsort.zip"])

    # Copy submission files to correct directory
    shutil.copyfile("LocalFileSorter.java", f"{ROOT_DIR}/src/main/java/com/github/hpides/exsort/LocalFileSorter.java")
    shutil.copyfile("RemoteFileSorter.java", f"{ROOT_DIR}/src/main/java/com/github/hpides/exsort/RemoteFileSorter.java")

    # Build submission
    os.chdir(os.path.join(Test_cwd, ROOT_DIR))
    gradle_build(job)

    # Check submission
    print("Checking submission...")
    is_valid = main(is_validate=True)
    output = "\n".join(msg for _, msg in output_lines)
    if is_valid:
        output += "\n======\nAll tests passed :)\n======"
        return job.send_pass_result(output)
    else:
        output += "\n======\nAt least one test failed :(\n======"
        return job.send_fail_result(output)

# -------------------------------------------------------------------------------


if __name__ == "__main__":
    valid = main()
    print("\n\nTest Results:\n=============")
    for okay, msg in output_lines:
        f = sys.stdout if okay else sys.stderr
        f.write("%s\n" % msg)
        f.flush()

    sys.exit(0 if valid else 1)
