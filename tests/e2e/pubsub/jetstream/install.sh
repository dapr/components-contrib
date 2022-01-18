#!/bin/sh
set -eu

# Copyright 2020-2022 The NATS Authors
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# We are sh, not bash; we might want bash/zsh for associative arrays but some
# OSes are currently on bash3 and removing bash, while we don't want a zsh
# dependency; so we're sticking to "pretty portable shell" even if it's a
# little more convoluted as a result.
#
# We rely upon the following beyond basic POSIX shell:
#  1. A  `local`  command (built-in to shell; almost all sh does have this)
#  2. A  `curl`   command (to download files)
#  3. An `unzip`  command (to extract content from .zip files)
#  4. A  `mktemp` command (to stage a downloaded zip-file)

# We rely upon naming conventions for release assets from GitHub to avoid
# having to parse JSON from their API, to avoid a dependency upon jq(1).
#
# <https://help.github.com/en/github/administering-a-repository/linking-to-releases>
# guarantees that:
#    /<owner>/<name>/releases/latest/download/<asset-name>.zip
# will be available; going via the API we got, for 'latest':
#    https://github.com/nats-io/nsc/releases/download/0.4.0/nsc-linux-amd64.zip
# ie:
#    /<owner>/<name>/releases/download/<release>/<asset-name>.zip
# Inspecting headers, the documented guarantee redirects to the API-returned
# URL, which redirects to the S3 bucket download URL.

# This is a list of the architectures we support, which should be listed in
# the Go architecture naming format.
readonly SUPPORTED_ARCHS="amd64 arm64"

# Finding the releases to download
readonly GITHUB_OWNER_REPO='nats-io/nsc'
readonly HTTP_USER_AGENT='nsc_install/0.2 (@nats-io)'

# Where to install to, relative to home-dir
readonly NSC_RELATIVE_BIN_DIR='.nsccli/bin'
# Binary name we are looking for (might have .exe extension on some platforms)
readonly NSC_BINARY_BASENAME='nsc'

progname="$(basename "$0" .sh)"
note() { printf >&2 '%s: %s\n' "$progname" "$*"; }
die() { note "$@"; exit 1; }

main() {
  parse_options "$@"
  shift $((OPTIND - 1))
  # error early if missing commands; put it after option processing
  # so that if we need to, we can add options to handle alternatives.
  check_have_external_commands

  # mkdir -m does not set permissions of parents; -v is not portable
  # We don't create anything private, so stick to inherited umask.
  mkdir -p -- "$opt_install_dir"

  zipfile_url="$(determine_zip_download_url)"
  [ -n "${zipfile_url}" ] || die "unable to determine a download URL"
  want_filename="$(exe_filename_per_os)"

  # The unzip command does not work well with piped stdin, we need to have
  # the complete zip-file on local disk.  The portability of mktemp(1) is
  # an unpleasant situation.
  # This is the sanest way to get a temporary directory which only we can
  # even look inside.
  old_umask="$(umask)"
  umask 077
  zip_dir="$(mktemp -d 2>/dev/null || mktemp -d -t 'ziptmpdir')" || \
    die "failed to create a temporary directory with mktemp(1)"
  umask "$old_umask"
  # POSIX does not give rm(1) a `-v` flag.
  trap "rm -rf -- '${zip_dir}'" EXIT

  stage_zipfile="${zip_dir}/$(zip_filename_per_os)"

  note "Downloading <${zipfile_url}>"
  curl_cmd --progress-bar --location --output "$stage_zipfile" "$zipfile_url"

  note "Extracting ${want_filename} from $stage_zipfile"
  # But unzip(1) does not let us override permissions and it does not obey
  # umask so the file might now exist with overly broad permissions, depending
  # upon whether or not the local environment has a per-user group which was
  # used.  We don't know that the extracting user wants everyone else in their
  # current group to be able to write to the file.
  # So: extract into the temporary directory, which we've forced via umask to
  # be self-only, chmod while it's safe in there, and then move it into place.
  #   -b is not in busybox, so we rely on unzip handling binary safely
  #   -j junks paths inside the zipfile; none expected, enforce that
  unzip -j -d "$zip_dir" "$stage_zipfile" "$want_filename"
  chmod 0755 "$zip_dir/$want_filename"
  # prompt the user to overwrite if need be
  mv -i -- "$zip_dir/$want_filename" "$opt_install_dir/./"

  link_and_show_instructions "$want_filename"
}

usage() {
  local ev="${1:-1}"
  [ "$ev" = 0 ] || exec >&2
  cat <<EOUSAGE
Usage: $progname [-t <tag>] [-d <dir>] [-s <dir>]
 -d dir     directory to download into [default: ~/$NSC_RELATIVE_BIN_DIR]
 -s dir     directory in which to place a symlink to the binary
            [default: ~/bin] [use '-' to forcibly not place a symlink]
 -t tag     retrieve a tagged release instead of the latest
 -a arch    force choosing a specific processor architecture [allowed: $SUPPORTED_ARCHS]
EOUSAGE
  exit "$ev"
}

opt_tag=''
opt_install_dir=''
opt_symlink_dir=''
opt_arch=''
parse_options() {
  while getopts ':a:d:hs:t:' arg; do
    case "$arg" in
      (h) usage 0 ;;

      (d) opt_install_dir="$OPTARG" ;;
      (s) opt_symlink_dir="$OPTARG" ;;
      (t) opt_tag="$OPTARG" ;;

      (a)
        if validate_arch "$OPTARG"; then
          opt_arch="$OPTARG"
        else
          die "unsupported arch for -a, try one of: $SUPPORTED_ARCHS"
        fi ;;

      (:) die "missing required option for -$OPTARG; see -h for help" ;;
      (\?) die "unknown option -$OPTARG; see -h for help" ;;
      (*) die "unhandled option -$arg; CODE BUG" ;;
    esac
  done

  if [ "$opt_install_dir" = "" ]; then
    opt_install_dir="${HOME:?}/${NSC_RELATIVE_BIN_DIR}"
  fi
  if [ "$opt_symlink_dir" = "" ] && [ -d "$HOME/bin" ]; then
    opt_symlink_dir="$HOME/bin"
  elif [ "$opt_symlink_dir" = "-" ]; then
    opt_symlink_dir=""
  fi
}

check_have_external_commands() {
  local cmd

  # Only those commands which take --help :
  for cmd in curl unzip
  do
    "$cmd" --help >/dev/null || die "missing command: $cmd"
  done

  # Our invocation of mktemp has to handle multiple variants; if that's not
  # installed, let it fail later.

  test -e /dev/stdin || die "missing device /dev/stdin"
}

normalized_ostype() {
  local ostype
  # We only need to worry about ASCII here
  ostype="$(uname -s | tr A-Z a-z)"
  case "$ostype" in
    (*linux*)  ostype="linux" ;;
    (win32)    ostype="windows" ;;
    (ming*_nt) ostype="windows" ;;
  esac
  printf '%s\n' "$ostype"
}

validate_arch() {
  local check="$1"
  local x
  # Deliberately not quoted, setting $@ within this function
  set $SUPPORTED_ARCHS
  for x; do
    if [ "$x" = "$check" ]; then
      return 0
    fi
  done
  return 1
}

normalized_arch() {
  # We are normalising to the Golang nomenclature, which is how the binaries are released.
  # The main ones are:  amd64 arm64
  # There is no universal standard here.  Go's is as good as any.

  # Command-line flag is the escape hatch.
  if [ -n "${opt_arch:-}" ]; then
    printf '%s\n' "$opt_arch"
    return 0
  fi

  # Beware `uname -m` vs `uname -p`.
  # Nominally, -m is machine, -p is processor.  But what does this mean in practice?
  # In practice, -m tends to be closer to the absolute truth of what the CPU is,
  # while -p is adaptive to personality, binary type, etc.
  # On Alpine Linux inside Docker, -p can fail `unknown` while `-m` works.
  #
  #                 uname -m    uname -p
  # Darwin/x86      x86_64      i386
  # Darwin/M1       arm64       arm
  # Alpine/docker   x86_64      unknown
  # Ubuntu/x86/64b  x86_64      x86_64
  # RPi 3B Linux    armv7l      unknown     (-m depends upon boot flags & kernel)
  #
  # SUSv4 requires that uname exist and that it have the -m flag, but does not document -p.
  local narch
  narch="$(uname -m)"
  case "$narch" in
    (x86_64) narch="amd64" ;;
    (amd64) true ;;
    (aarch64) narch="arm64" ;;
    (arm64) true ;;
    (*) die "Unhandled architecture '$narch', use -a flag to select a supported arch" ;;
  esac
  if validate_arch "$narch"; then
    printf '%s\n' "$narch"
  else
    die "Unhandled architecture '$narch', use -a flag to select a supported arch"
  fi
}

zip_filename_per_os() {
  # We break these out into a separate variable instead of passing directly
  # to printf, so that if there's a failure in normalization then the printf
  # won't swallow the exit status of the $(...) subshell and will instead
  # abort correctly.
  local zipname
  zipname="nsc-$(normalized_ostype)-$(normalized_arch).zip" || exit $?
  printf '%s\n' "${zipname}"
}

exe_filename_per_os() {
  local fn="$NSC_BINARY_BASENAME"
  case "$(normalized_ostype)" in
    (windows) fn="${fn}.exe" ;;
  esac
  printf '%s\n' "$fn"
}

curl_cmd() {
  curl --user-agent "$HTTP_USER_AGENT" "$@"
}

determine_zip_download_url() {
  local want_filename download_url

  want_filename="$(zip_filename_per_os)"
  if [ -n "$opt_tag" ]; then
    printf 'https://github.com/%s/releases/download/%s/%s\n' \
      "$GITHUB_OWNER_REPO" "$opt_tag" "$want_filename"
  else
    printf 'https://github.com/%s/releases/latest/download/%s\n' \
      "$GITHUB_OWNER_REPO" "$want_filename"
  fi
}

dir_is_in_PATH() {
  local needle="$1"
  local oIFS="$IFS"
  local pathdir
  case "$(normalized_ostype)" in
    (windows) IFS=';' ;;
    (*)       IFS=':' ;;
  esac
  set $PATH
  IFS="$oIFS"
  for pathdir
  do
    if [ "$pathdir" = "$needle" ]; then
      return 0
    fi
  done
  return 1
}

# Returns true if no further installation instructions are needed;
# Returns false otherwise.
maybe_make_symlink() {
  local target="${1:?need a file to link to}"
  local symdir="${2:?need a directory within which to create a symlink}"
  local linkname="${3:?need a name to give the symlink}"

  if ! [ -d "$symdir" ]; then
    note "skipping symlink because directory does not exist: $symdir"
    return 1
  fi
  # ln(1) `-v` is busybox but is not POSIX
  if ! ln -sf -- "$target" "$symdir/$linkname"
  then
    note "failed to create a symlink in: $symdir"
    return 1
  fi
  ls -ld -- "$symdir/$linkname"
  if dir_is_in_PATH "$symdir"; then
    note "Symlink dir '$symdir' is already in your PATH"
    return 0
  fi
  note "Symlink dir '$symdir' is not in your PATH?"
  return 1
}

link_and_show_instructions() {
  local new_cmd="${1:?need a command which has been installed}"

  local target="$opt_install_dir/$new_cmd"

  echo
  note "NSC: $target"
  ls -ld -- "$target"

  if [ -n "$opt_symlink_dir" ]; then
    if maybe_make_symlink "$target" "$opt_symlink_dir" "$new_cmd"
    then
      return 0
    fi
  fi

  echo
  printf 'Now manually add %s to your $PATH\n' "$opt_install_dir"

  case "$(normalized_ostype)" in
    (windows) cat <<EOWINDOWS ;;
Windows Cmd Prompt Example:
  setx path %path;"${opt_install_dir}"

EOWINDOWS

    (*) cat <<EOOTHER ;;
Bash Example:
  echo 'export PATH="\${PATH}:${opt_install_dir}"' >> ~/.bashrc
  source ~/.bashrc

Zsh Example:
  echo 'path+=("${opt_install_dir}")' >> ~/.zshrc
  source ~/.zshrc

EOOTHER

  esac
}

main "$@"
