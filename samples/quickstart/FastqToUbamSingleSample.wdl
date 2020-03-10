workflow FastqToUbamSingleSample {

    Array[File] fastq_pair
   
    String? gotc_docker_override
    String gotc_docker = select_first([gotc_docker_override, "broadinstitute/genomes-in-the-cloud:2.3.1-1512499786"])
    String? gotc_path_override
    String gotc_path = select_first([gotc_path_override, "/usr/gitc/"])

    String sample_name
    String library_name 
    String group_name 
    String platform 
    String platform_unit


    call FastqToUbam { 
    input:
        fastqs = fastq_pair,
        output_bam_basename = sample_name + ".unmapped" ,
        group = group_name,
        sample = sample_name,
        library = library_name,
        platform = platform,
        platform_unit = platform_unit,
        docker_image = gotc_docker,
        gotc_path = gotc_path
    }
    output {
    File unmapped_bam = FastqToUbam.unmapped_bam
    }
}

task FastqToUbam {

    Array[File] fastqs
    String output_bam_basename

    String group
    String sample
    String library
    String platform
    String platform_unit
    
    String docker_image
    String gotc_path

    Int disk_size = ceil(size(${fastqs[0]}, "GB") + size(${fastqs[1]}, "GB")) * 2.5 

    command {
        java  -jar ${gotc_path}picard.jar FastqToSam \
            FASTQ=${fastqs[0]} \
            FASTQ2=${fastqs[1]} \
            OUTPUT=${output_bam_basename}.bam \
            READ_GROUP_NAME=${group} \
            SAMPLE_NAME=${sample} \
            LIBRARY_NAME=${library} \
            PLATFORM_UNIT=${platform_unit} \
            PLATFORM=${platform}
    }

    runtime {
        docker: docker_image
        memory: "32 GB"
        cpu: "16"
        disk: disk_size + " GB"
    }

    output {
        File unmapped_bam = "${output_bam_basename}.bam"
    }
}
