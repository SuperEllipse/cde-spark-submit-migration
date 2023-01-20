#!/bin/bash

# set -e

CLOUDERA_BIN=/opt/cloudera/bin
OSTYPE=$(uname)
echo "os type: ${OSTYPE}"

subcommand_usage() {
    echo "Usage: ${0}
    Sub-commands:
        enable              Enable drop-in replacement migration to CDE (admin)
        disable             Disable drop-in replacement migration to CDE (admin)
        add-profile         Add profile needed to access CDE (user)
        help                Print this message"
}

subcommand_enable() {
    while getopts "f:" options
    do
        case ${options} in
            (f)
                FORM_FACTOR=${OPTARG}
                ;;
        esac
    done

    if [ ! -d ${CLOUDERA_BIN} ]; then
        mkdir -p ${CLOUDERA_BIN}
        chmod o+rx ${CLOUDERA_BIN}
    fi

    # install cde binary
    cp cde ${CLOUDERA_BIN}/
    chmod o+rx ${CLOUDERA_BIN}/cde
    echo "cde installed at ${CLOUDERA_BIN}/cde"

    # install cde-env.sh shell script
    cp cde-env.sh ${CLOUDERA_BIN}/
    chmod o+rx ${CLOUDERA_BIN}/cde-env.sh
    echo "cde-env.sh installed at ${CLOUDERA_BIN}/cde-env.sh"

    # install spark-submit-cde proxy script
    cp spark-submit-cde ${CLOUDERA_BIN}/
    chmod o+rx ${CLOUDERA_BIN}/spark-submit-cde
    echo "spark-submit-cde installed at ${CLOUDERA_BIN}/spark-submit-cde"

    # install spark3-submit-cde proxy script
    cp spark3-submit-cde ${CLOUDERA_BIN}/
    chmod o+rx ${CLOUDERA_BIN}/spark3-submit-cde
    echo "spark3-submit-cde installed at ${CLOUDERA_BIN}/spark3-submit-cde"

    # customize spark-submit-cde script
    if command -v spark-submit &> /dev/null; then
        spark_submit_yarn=$(which spark-submit)
        echo "spark-submit path: ${spark_submit_yarn}"
        if [ $OSTYPE == "Darwin" ]; then
            spark_submit_yarn=$(readlink ${spark_submit_yarn})
            echo "yarn spark-submit path: ${spark_submit_yarn}"
            sed -i '' "s#YARN_SPARK_SUBMIT=replace_with_yarn_spark_submit_path#YARN_SPARK_SUBMIT=${spark_submit_yarn}#g" ${CLOUDERA_BIN}/spark-submit-cde
            sed -i '' "s#CDE_BIN_PATH=\$(which cde)#CDE_BIN_PATH=${CLOUDERA_BIN}/cde#g" ${CLOUDERA_BIN}/spark-submit-cde
            if [ ${FORM_FACTOR} == "private" ]; then
                sed -i '' 's/exec ${CDE_BIN_PATH}\/cde/printf 'yes' | exec ${CDE_BIN_PATH}\/cde/g' ${CLOUDERA_BIN}/spark-submit-cde
            fi
        else
            spark_submit_yarn=$(readlink -f ${spark_submit_yarn})
            echo "yarn spark-submit path: ${spark_submit_yarn}"
            sed -i "s#YARN_SPARK_SUBMIT=replace_with_yarn_spark_submit_path#YARN_SPARK_SUBMIT=${spark_submit_yarn}#g" ${CLOUDERA_BIN}/spark-submit-cde
            sed -i "s#CDE_BIN_PATH=\$(which cde)#CDE_BIN_PATH=${CLOUDERA_BIN}/cde#g" ${CLOUDERA_BIN}/spark-submit-cde
            if [ ${FORM_FACTOR} == "private" ]; then
                sed -i 's/exec ${CDE_BIN_PATH}\/cde/printf 'yes' | exec ${CDE_BIN_PATH}\/cde/g' ${CLOUDERA_BIN}/spark-submit-cde
            fi
        fi
    fi

    # customize spark3-submit-cde script
    if command -v spark3-submit &> /dev/null; then
        spark_submit_yarn=$(which spark3-submit)
        echo "spark3-submit path: ${spark_submit_yarn}"
        if [ $OSTYPE == "Darwin" ]; then
            spark_submit_yarn=$(readlink ${spark_submit_yarn})
            echo "yarn spark3-submit path: ${spark_submit_yarn}"
            sed -i '' "s#YARN_SPARK_SUBMIT=replace_with_yarn_spark_submit_path#YARN_SPARK_SUBMIT=${spark_submit_yarn}#g" ${CLOUDERA_BIN}/spark3-submit-cde
            sed -i '' "s#CDE_BIN_PATH=\$(which cde)#CDE_BIN_PATH=${CLOUDERA_BIN}/cde#g" ${CLOUDERA_BIN}/spark3-submit-cde
            if [ ${FORM_FACTOR} == "private" ]; then
                sed -i '' 's/exec ${CDE_BIN_PATH}\/cde/printf 'yes' | exec ${CDE_BIN_PATH}\/cde/g' ${CLOUDERA_BIN}/spark3-submit-cde
            fi
        else
            spark_submit_yarn=$(readlink -f ${spark_submit_yarn})
            echo "yarn spark3-submit path: ${spark_submit_yarn}"
            sed -i "s#YARN_SPARK_SUBMIT=replace_with_yarn_spark_submit_path#YARN_SPARK_SUBMIT=${spark_submit_yarn}#g" ${CLOUDERA_BIN}/spark3-submit-cde
            sed -i "s#CDE_BIN_PATH=\$(which cde)#CDE_BIN_PATH=${CLOUDERA_BIN}/cde#g" ${CLOUDERA_BIN}/spark3-submit-cde
            if [ ${FORM_FACTOR} == "private" ]; then
                sed -i 's/exec ${CDE_BIN_PATH}\/cde/printf 'yes' | exec ${CDE_BIN_PATH}\/cde/g' ${CLOUDERA_BIN}/spark3-submit-cde
            fi
        fi
    fi

    # create symlinks
    echo "about to create symlinks"
    ln -s ${CLOUDERA_BIN}/spark-submit-cde ${CLOUDERA_BIN}/spark-submit
    ln -s ${CLOUDERA_BIN}/spark3-submit-cde ${CLOUDERA_BIN}/spark3-submit
    echo "finish creating symlinks"
}

subcommand_disable() {
    if ! command -v cde-env.sh &> /dev/null; then
        echo "cde-env-tool is not enabled. quit"
        exit 0
    fi
    cde_env=$(which cde-env.sh)
    unlink $cde_env
    cde=$(which cde)
    unlink $cde
    unlink ${CLOUDERA_BIN}/spark-submit
    unlink ${CLOUDERA_BIN}/spark3-submit
    rm ${CLOUDERA_BIN}/cde
    rm ${CLOUDERA_BIN}/cde-env.sh
    rm ${CLOUDERA_BIN}/spark-submit-cde
    rm ${CLOUDERA_BIN}/spark3-submit-cde
}

write_to_config() {
    cp ${CREDENTIAL} ${HOME}/.cde/credentials
    chmod 600 ${HOME}/.cde/credentials
    if [ -z "${FORM_FACTOR}" ]; then
        cat << EOF > ${HOME}/.cde/config.yaml
vcluster-endpoint: ${VC_ENDPOINT}
cdp-endpoint: ${CDP_ENDPOINT}
credentials-file: ${HOME}/.cde/credentials
allow-all-spark-submit-flags: true
kerberos: true
EOF
    elif [ "${FORM_FACTOR}" == "private" ]; then
        cat << EOF > ${HOME}/.cde/config.yaml
vcluster-endpoint: ${VC_ENDPOINT}
cdp-endpoint: ${CDP_ENDPOINT}
credentials-file: ${HOME}/.cde/credentials
tls-insecure: true
allow-all-spark-submit-flags: true
kerberos: true
EOF
    else
        echo "The form factor: ${FORM_FACTOR} not supported, abort"
        exit 1
    fi
}

subcommand_add_profile() {
    if [ $# -eq 0 ]
    then
        echo -e "No arguments supplied\nUsage: $0 add-profile -n <profile-name> -f <form-factor> -v <vc-end-point> -c <credentials-location> -p <cdp-end-point>"
        exit 1
    fi

    while getopts "n:v:c:p:f:" options
    do
        case ${options} in
            (n)
                PROFILE=${OPTARG}
                echo "Profile name: ${PROFILE}"
                ;;
            (v)
                VC_ENDPOINT=${OPTARG}
                echo "VC endpoint: ${VC_ENDPOINT}"
                ;;
            (c)
                CREDENTIAL=${OPTARG}
                ;;
            (p)
                CDP_ENDPOINT=${OPTARG}
                ;;
            (f)
                FORM_FACTOR=${OPTARG}
                ;;
        esac
    done

    if [ -z "${VC_ENDPOINT}" ]; then
        echo "Missing virtual cluster endpoint. Use -v"
        exit 1
    fi

    if [ -z "${CREDENTIAL}" ]; then
        echo "Missing credential location. Use -c"
        exit 1
    else
        if [ ! -f "${CREDENTIAL}" ]; then
            echo "The credential file does not exist: ${CREDENTIALS}. Abort"
            exit 1
        fi
    fi

    if [ -z "${CDP_ENDPOINT}" ]; then
        echo "Missing cdp endpoint. Use -p"
        exit 1
    fi

    if [ -d "${HOME}/.cde" ]; then
        echo "${HOME}/.cde already exists, abort"
        exit 1
    fi

    mkdir ${HOME}/.cde
    write_to_config
    echo "the add-profile subcommand is called"
}

subcommand_update_profile() {
    echo "the update-profile subcommand is called"
}

subcommand_activate() {
    if [ $# -eq 0 ]; then
        echo -e "No arguments supplied\nUsage: $0 activate -p <profile-name>"
        exit 1
    fi

    while getopts "p:" options
    do
        case ${options} in
            (p)
                PROFILE=${OPTARG}
                ;;
        esac
    done

    if [ -z "${PROFILE}" ]; then
        echo "PROFILE missing. Use -p"
        exit 1
    fi

    echo "${PROFILE}" > ${HOME}/.cde/spark-submit-profile
    if [ ! -z ${CDE_CONFIG_PROFILE+x} ]; then
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
        echo "!!! the CDE_CONFIG_PROFILE env variable is set.      !!!"
        echo "!!! Please unset or update this variable in order    !!!"
        echo "!!! for your new profile to take effect.             !!!"
        echo "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!"
    fi
}

main() {
    subcommand="$1"
    if [ x"${subcommand}"x == "xx" ]; then
        subcommand="help"
    else
        shift
    fi

    case $subcommand in
        help)
            subcommand_usage
            ;;
        enable-spark-submit-proxy)
            subcommand_enable "$@"
            ;;
        disable-spark-submit-proxy)
            subcommand_disable "$@"
            ;;
        activate)
            subcommand_activate "$@"
            ;;
        add-profile)
            subcommand_add_profile "$@"
            ;;
        update-profile)
            subcommand_update_profile "$@"
            ;;
            

    esac
}

main "$@"