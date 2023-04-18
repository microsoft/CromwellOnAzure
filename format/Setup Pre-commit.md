# Set up Pre-commit
To set up dotnet format to run locally on 'git commit', you need to perform the following actions. You should only need to do this once unless you remove and clone the repo again. 

## Git pre-commit hook to reformat
Navigate to GIT-REPO_LOCATION/format. Run the "dotnet-format-pre-commit.cmd" to copy the pre-commit file into the GIT-REPO_LOCATION/.git/hooks directory. Verify that the file has been copied as expeted into GIT-REPO_LOCATION/.git/hooks. The cmd script assumes it is being run from the **GIT-REPO_LOCATION/format** directory
  
```
cd GIT-REPO_LOCATION/format
.\dotnet-format-pre-commit.cmd
```

After running this, when you go to commit, dotnet format should run automatically and correct any formatting that needs attention. 
