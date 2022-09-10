## Cromwell on AKS Instructions and Troubleshooting

### Deployment Dependencies
The CoA deployer requires the user to have Helm 3 installed locally to deploy with AKS. Use the flag "--HelmExePath HELM_PATH" to give the deployer the path to the helm exe, if no flag is passed the deployer will assume Helm is installed with Chocolatey at "C:\\ProgramData\\chocolatey\\bin\\helm.exe".

### Deployment Models

- ### CoA provisioned AKS account
    Add the flag "--UseAks true" and the deployer will provision an AKS account and run its containers in AKS rather than provisioning a VM. 
- ### Shared AKS account with CoA namespace
    Add the flags "--UseAks true --AksClusterName {existingClusterName}", where the user has admin access to the existing AKS account, the deployer will deploy blob-csi-driver, and aad-pod-identity to the kube-system namespace, and then deploy CoA to the namespace "coa". Add the flag "--AksCoANamespace {namespace}" to override the default namespace.
- ### Shared AKS account without developer access. 
    If the user is required to use an AKS account, but does not have the required access, the deployer will produce a Helm chart that can then be installed by an admin or existing CI/CD pipeline. Add the flags "--UseAks true --ManualHelmDeployment". The deployer will print a postgresql command, this would typically be run on the kubernetes node to setup the cromwell user however the user will need to run this manually since the deployer won't directly access the AKS account. 

    - Run the deployer with supplied flags. 
    - Deployer will create initial resources and pause once it's time to deploy the Helm chart.
    - Ensure the blob-csi-driver and aad-pod-identity are installed.
    - Install the CoA Helm chart. 
    - Run the postgresql command to create the cromwell user. 
    - Press enter in the deployer console to finish the deployment and run a test workflow. 

### Depedent Kubernetes Packages
These packages will be deployed into the kube-system namespace.
- ### Blob CSI Driver - https://github.com/kubernetes-sigs/blob-csi-driver/
    This is used to mount the storage account to the containers.
- ### AAD Pod Identity - https://github.com/Azure/aad-pod-identity
    This is used to assigned managed identities to the containers. 

### External storage accounts
Typically in CromwellOnAzure you can add storage accounts with input data to the containers-to-mount file. For AKS, you need to modify the values.yaml file of the helm chart and redeploy. The vaules-template.yaml will have examples externalContainers and externalSasContainers. 

### Logs and troubleshooting
For troubleshooting any of the CoA services, you can login directly to the pods or get logs using the kubectl program. The deployer will write a kubeconfig to the working directory, either copy that file to ~/.kube/config for reference it manually for each command with --kubeconfig {coa-directory}/kubeconfig.txt. You can also run the command `az aks get-credentials --resource-group {coa-resource-group} --name {aks-count} --subscription {subscription-id} --file kubeconfig.txt` to get the file.

1. Get the exact name of the pods. 

    `kubectl get pods --namespace coa`
2. Get logs for the tes pod.

    `kubectl logs tes-68d6dc4789-mvvwj --namespace coa`
3. SSH to pod to troubleshoot storage or network connectivity.

    `kubectl exec --namespace coa --stdin --tty tes-68d6dc4789-mvvwj -- /bin/bash`

### Updating settings and environment variables.

For VM based CoA deployments, you can ssh into the VM host, update the environment files, and restart the VM. 
To update update settings for AKS, you will need to redeploy the helm chart. If you still have the chart locally, 
you can update the values.yaml file and redeploy with:

`helm upgrade cromwellonazure ./scripts/helm --kubeconfig kubeconfig.txt --namespace coa`

If the original chart is lost, you can regenerate it by running the deployer again with the "--update true" and "--AksClusterName {existingClusterName}" flags. 