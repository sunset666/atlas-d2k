#! /bin/bash

# set -x  # for logging and debugging

# allowed values of NIDDK_INSTANCE
niddk_instance_strings=" prod dev "

# function to find the path to this script
function get_dir_of_this_script () {
    # This function sets DIR to the directory in which this script itself is found.
    # Thank you https://stackoverflow.com/questions/59895/how-to-get-the-source-directory-of-a-bash-script-from-within-the-script-itself
    SCRIPT_SOURCE="${BASH_SOURCE[0]}"
    while [ -h "$SCRIPT_SOURCE" ]; do # resolve $SCRIPT_SOURCE until the file is no longer a symlink
	DIR="$( cd -P "$( dirname "$SCRIPT_SOURCE" )" >/dev/null 2>&1 && pwd )"
	SCRIPT_SOURCE="$(readlink "$SCRIPT_SOURCE")"
	# if $SCRIPT_SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
	[[ $SCRIPT_SOURCE != /* ]] && SCRIPT_SOURCE="$DIR/$SCRIPT_SOURCE" 
    done
    DIR="$( cd -P "$( dirname "$SCRIPT_SOURCE" )" >/dev/null 2>&1 && pwd )"
    }

# function to check for presence of token $2 in list $1
contains() {
    [[ $1 =~ (^|[[:space:]])$2($|[[:space:]]) ]] && echo 1 || echo 0
}


# check for instance info
if [[ -z "${NIDDK_INSTANCE}" ]] ; then
    echo "NIDDK_INSTANCE is not defined"
    exit -1
else
    instance="${NIDDK_INSTANCE}"
fi
if [ $(contains "${niddk_instance_strings}" "${instance}") == 0 ] ; then
   echo "${instance} is not one of ${niddk_instance_strings}"
   exit -1
fi

# set DIR to the directory of the current script, and find the source tree top level
get_dir_of_this_script
cd "$DIR"
top_level_dir="$(git rev-parse --show-toplevel)"

ENV_SCRIPT="/airflow_environments/env_${NIDDK_INSTANCE}.sh"

. "$(dirname "$(readlink -f "$0")")${ENV_SCRIPT}"

# Handle setting of environment variables.
#
# The goal is to let values from the environment (prefix NIDDK_) override
# those from the config files (prefix AF_).  We also check that all
# required variables are set at some level.
envvars=( CONFIG HOME \
	)
for varname in "${envvars[@]}" ; do
    full_varname="AIRFLOW_${varname}"
    cfg_varname="AF_${varname}"
    if [[ -z "${!full_varname}" ]] ; then
	export ${full_varname}=${!cfg_varname}
    fi
    if [[ -z "${!full_varname}" ]] ; then
	echo "${full_varname} is not set"
	exit -1
    fi
done

if [ "${AF_METHOD}" == 'conda' ] ; then
    which conda || export PATH=/opt/anaconda3/bin:$PATH
    eval "$(conda shell.bash hook)"
    conda activate "${AF_ENV_NAME}"
elif [ "${AF_METHOD}" == 'module_conda' ] ; then
    source /etc/profile.d/modules.sh
    module use /hive/modulefiles
    module load anaconda
    eval "$(conda shell.bash hook)"
    conda activate "${AF_ENV_NAME}"
elif [ "${AF_METHOD}" == 'venv' ] ; then
    source "${AF_ENV_NAME}/bin/activate"
else
    echo "unknown AF_METHOD ${AF_METHOD}"
    exit -1
fi
echo 'PATH follows'
echo $PATH
echo 'PYTHONPATH follows'
echo $PYTHONPATH
echo 'Environment follows'
printenv

cd $AIRFLOW_HOME ; \
env AIRFLOW__NIDDK_API_PLUGIN__BUILD_NUMBER="$(cat ${top_level_dir}/build_number)" \
    airflow $*