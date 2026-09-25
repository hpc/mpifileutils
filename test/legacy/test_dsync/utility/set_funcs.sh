#set -x

function union() {
	funcname=union
	if [[ $# -lt 2 ]] || [[ ! -f $1 ]] || [[ ! -f $2 ]]; then
		echo "$0: ${funcname}: requires 2 arguments, both files: $*"
		exit
	fi

	sort -u <(sort $1) <(sort $2)
}

function intersection() {
	funcname=intersection
	if [[ $# -lt 2 ]] || [[ ! -f $1 ]] || [[ ! -f $2 ]]; then
		echo "$0: ${funcname}: requires 2 arguments, both files: $*"
		exit
	fi

	comm -12 <(sort $1) <(sort $2)
}

function sets_equal() {
	funcname=sets_equal
	if [[ $# -lt 2 ]] || [[ ! -f $1 ]] || [[ ! -f $2 ]]; then
		echo "$0: ${funcname}: requires 2 arguments, both files: $*"
		exit
	fi

	diff -q <(sort $1) <(sort $2) >/dev/null
}
