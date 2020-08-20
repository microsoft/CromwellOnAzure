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
    docker: 'ubuntu:18.04'
    preemptible: true
  }
}

workflow test {
  call hello
}