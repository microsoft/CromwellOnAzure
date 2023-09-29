task CreateUUID {
  command {
    uuidgen > uuid_output.txt
  }

  output {
    File uuidFile = "uuid_output.txt"
  }

  runtime {
    docker: 'mcr.microsoft.com/mirror/docker/library/ubuntu:22.04'
    preemptible: true
    workflow_execution_identity: "workflow_execution_identity"
  }
}

workflow test {
  call CreateUUID
}
