task globtest {

  command {
    echo "hello1" > output1.txt && echo "hello2" > output2.txt && echo "hello3" > output3.txt
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
