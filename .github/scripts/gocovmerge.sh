#!/bin/bash
conf_code_cov=$1
cert_code_cov=$2
final_code_cov=$3

mkdir -p ${final_code_cov}

for file in ${cert_code_cov}/*
do
    cert_file=$(basename ${file})
    conf_files=$(find ${conf_code_cov} -iname "${cert_file%.out}*")
    if [ -z "$conf_files" ]; then
        echo "Copying ${cert_code_cov}/${cert_file} to ${final_code_cov}/${cert_file}"
        cp ${cert_code_cov}/${cert_file} ${final_code_cov}/${cert_file}
    else
        echo "${conf_files}" | while read -r line
        do
            conf_file=$(basename ${line})
            echo Merging ${cert_code_cov}/${cert_file} and ${conf_code_cov}/${conf_file} into ${final_code_cov}/${conf_file}
            gocovmerge ${cert_code_cov}/${cert_file} ${conf_code_cov}/${conf_file} >> ${final_code_cov}/${conf_file}
        done
    fi
done

for file in ${conf_code_cov}/*
do
    conf_file=$(basename ${file})
    final_file="${final_code_cov}/${conf_file}"
    if [ ! -f "${final_file}" ]; then
        echo Copying ${conf_code_cov}/${conf_file} to ${final_code_cov}/${conf_file}
        cp ${conf_code_cov}/${conf_file} ${final_code_cov}/${conf_file}
    fi
done
