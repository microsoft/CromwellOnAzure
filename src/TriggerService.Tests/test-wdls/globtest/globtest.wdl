task globtest {

  command {
    cat $HOSTNAME > output1.txt && cat $HOSTNAME > output2.txt && cat $HOSTNAME > output3.txt
  }
  output {
    Array[File] output_textfiles = glob("*.txt")
  }
  runtime {
    docker: 'mcr.microsoft.com/mirror/docker/library/ubuntu:22.04'
    preemptible: true
  }
}

workflow test {
  call globtest
}
