task globtest {

  command {
    echo "hello1" > output1.txt && echo "hello2" > output2.txt && echo "hello3" > output3.txt
  }
  output {
    Array[File] output_textfiles = glob("*.txt")
  }
  runtime {
    docker: 'coaintegrationtest.azurecr.io/ubuntu:latest'
    preemptible: true
  }
}

workflow test {
  call globtest
}
