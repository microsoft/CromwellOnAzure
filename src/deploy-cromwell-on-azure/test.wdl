task hello {
  String name
  File inputFile

  command {
    echo 'Hello ${name}!'
    cat ${inputFile} > outfile2.txt
  }
  output {
    File outfile1 = stdout()
    File outfile2 = "outfile2.txt"
  }
  runtime {
    docker: 'mcr.microsoft.com/mirror/docker/library/ubuntu:22.04'
    preemptible: true
  }
}

workflow test {
  call hello
}
