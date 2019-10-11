task hello {
  String name

  command {
    echo 'Hello ${name}!'
  }
  output {
	File response = stdout()
  }
  runtime {
	docker: 'ubuntu:16.04'
  }
}

workflow test {
  call hello
}