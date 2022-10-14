#!/bin/bash
# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.

echo "Run Common Workflows starting"
resource_group_name="coainttest-$(Build.BuildNumber)"
storage_account_name_prefix="coainttest"

testWorkingDirectory=$(System.DefaultWorkingDirectory)

echo "Getting the storage account in resource group name: $resource_group_name."
storageAccountName=`az storage account list \
    --resource-group $resource_group_name \
    --query "[?starts_with(name,'$storage_account_name_prefix')].name | [0]" \
    -o tsv`

echo "Getting the storage account key for account $storageAccountName, resource group name: $resource_group_name."
accountKey=`az storage account keys list \
    --account-name $storageAccountName \
    --resource-group $resource_group_name \
    --query "[].value | [0]" \
    -o tsv`

echo "storage account $storageAccountName"

function dowloadAndSubmitTriggerFile {
    local testTriggerFileSource=$1
    local testFileName=$2

    echo "Setting up the destination path for trigger file in ${testWorkingDirectory}."
    local testTriggerFilePath="${testWorkingDirectory}/${testFileName}"
    echo "Trigger file path: $testTriggerFilePath."

    echo "Download workflow trigger file."
    curl -o $testTriggerFilePath $testTriggerFileSource

    echo "Upload workflow trigger file to workflows/new."
    az storage blob upload -c workflows/new --file ${testTriggerFilePath} --name ${testFileName} --account-name ${storageAccountName} --account-key ${accountKey}
}

function checkProcessingOfTriggerFile {
    local testFileName=$1

    echo "Waiting for workflow to start."
    for i in `seq 1 10`;
    do
        echo "Checking for blob inprogress/${testFileName}"
    
        inprogress=`az storage blob list -c workflows \
            --prefix "inprogress/${testFileName}" \
            --query "[] | length(@)" \
            --account-name $storageAccountName \
            --account-key $accountKey`
        if [[ $inprogress -eq 1 ]]
        then
           echo "Workflow trigger file is in progress."
           break
        fi
        innew=`az storage blob list -c workflows \
            --prefix "new/${testFileName}" \
            --query "[] | length(@)" \
            --account-name $storageAccountName \
            --account-key $accountKey`
        if [[ $innew -eq 1 ]]
        then
           echo "Workflow file is in new. Waiting for progress to start..."
        fi

        sleep 100
    done

    echo "Waiting for workflow result."
    while :
    do
        echo "Checking for blob succeeded/${testFileName}"
        result=`az storage blob list -c workflows \
            --prefix "succeeded/${testFileName}" \
            --query "[] | length(@)" \
            --account-name $storageAccountName \
            --account-key $accountKey`
        if [[ $result -eq 1 ]]
        then
            echo "Workflow has succeeded."
            break
        fi
  
        echo "Checking for blob failed/${testFileName}"
        failed=`az storage blob list -c workflows \
            --prefix "failed/${testFileName}" \
            --query "[] | length(@)" \
            --account-name $storageAccountName \
            --account-key $accountKey`
        if [[ $failed -eq 1 ]]
        then
            echo "Workflow has timed out to start."
            break
        fi

        stillinprogress=`az storage blob list -c workflows \
            --prefix "inprogress/${testFileName}" \
            --query "[] | length(@)" \
            --account-name $storageAccountName \
            --account-key $accountKey`
       if [[ $stillinprogress -eq 1 ]]
       then
           echo "Workflow is in progress"
           sleep 100
       else
           echo "Error: Workflow is not run. Diagnostic print of Workflows container:"
           az storage blob list -c workflows \
                --account-name $storageAccountName \
                --account-key $accountKey
           break
       fi
    done 
    (( result-=1 ))
    echo "Returning exit code $result"
    return $result
}

echo "Data pre-processing for variant discovery"
testTriggerFileSource1='https://raw.githubusercontent.com/microsoft/gatk4-data-processing-azure/main-azure/processing-for-variant-discovery-gatk4.hg38.trigger.json'
testFileName1="processing-for-variant-discovery-gatk4.hg38.trigger"
testTriggerFileSource2='https://raw.githubusercontent.com/microsoft/gatk4-data-processing-azure/main-azure/processing-for-variant-discovery-gatk4.b37.trigger.json'
testFileName2="processing-for-variant-discovery-gatk4.b37.trigger"
dowloadAndSubmitTriggerFile ${testTriggerFileSource1} ${testFileName1}.json
dowloadAndSubmitTriggerFile ${testTriggerFileSource2} ${testFileName2}.json
checkProcessingOfTriggerFile $testFileName1
result1=$?
checkProcessingOfTriggerFile $testFileName2
result2=$?

echo "Germline short variant discovery (SNPs + Indels)"
testTriggerFileSource3='https://raw.githubusercontent.com/microsoft/gatk4-genome-processing-pipeline-azure/main-azure/WholeGenomeGermlineSingleSample.trigger.json'
testFileName3="WholeGenomeGermlineSingleSample.trigger"
dowloadAndSubmitTriggerFile ${testTriggerFileSource3} ${testFileName3}.json
checkProcessingOfTriggerFile $testFileName3
result3=$?

echo "Somatic short variant discovery (SNVs + Indels)"
testTriggerFileSource4='https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2.trigger.json'
testFileName4="mutect2.trigger"
testTriggerFileSource5='https://raw.githubusercontent.com/microsoft/gatk4-somatic-snvs-indels-azure/main-azure/mutect2_pon.trigger.json'
testFileName5="mutect2_pon.trigger"
dowloadAndSubmitTriggerFile ${testTriggerFileSource4} ${testFileName4}.json
dowloadAndSubmitTriggerFile ${testTriggerFileSource5} ${testFileName5}.json
checkProcessingOfTriggerFile $testFileName4
result4=$?
checkProcessingOfTriggerFile $testFileName5
result5=$?

echo "RNAseq short variant discovery (SNPs + Indels)"
testTriggerFileSource6='https://raw.githubusercontent.com/microsoft/gatk4-rnaseq-germline-snps-indels-azure/main-azure/gatk4-rna-germline-variant-calling.trigger.json'
testFileName6="gatk4-rna-germline-variant-calling.trigger"
dowloadAndSubmitTriggerFile ${testTriggerFileSource6} ${testFileName6}.json
checkProcessingOfTriggerFile $testFileName6
result6=$?

echo "Variant-filtering with Convolutional Neural Networks"
testTriggerFileSource7='https://raw.githubusercontent.com/microsoft/gatk4-cnn-variant-filter-azure/main-azure/cram2filtered.trigger.json'
testFileName7="cram2filtered.trigger"
dowloadAndSubmitTriggerFile ${testTriggerFileSource7} ${testFileName7}.json
checkProcessingOfTriggerFile $testFileName7
result7=$?

echo "Sequence data format conversion"
testTriggerFileSource8='https://raw.githubusercontent.com/microsoft/seq-format-conversion-azure/main-azure/interleaved-fastq-to-paired-fastq.trigger.json'
testFileName8="interleaved-fastq-to-paired-fastq.trigger"
testTriggerFileSource9='https://github.com/microsoft/seq-format-conversion-azure/blob/main-azure/bam-to-unmapped-bams.trigger.json'
testFileName9="bam-to-unmapped-bams.trigger"
dowloadAndSubmitTriggerFile ${testTriggerFileSource8} ${testFileName8}.json
checkProcessingOfTriggerFile $testFileName8
result8=$?

dowloadAndSubmitTriggerFile ${testTriggerFileSource9} ${testFileName9}.json
checkProcessingOfTriggerFile $testFileName9
result9=$?

exit result1+result2+result3+result4+result5+result6+result7+result8+result9