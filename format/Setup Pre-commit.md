# Set up Pre-commit
To set up dotnet format to run locally on 'git commit', you need to perform the following actions. You should only need to do this once unless you remove and clone the repo again. 

## Git pre-commit hook to reformat
Navigate to GIT-REPO-LOCATION/format. Run the "dotnet-format-pre-commit.cmd" to copy the pre-commit file into the GIT-REPO-LOCATION/.git/hooks directory. Verify that the file has been copied as expeted into GIT-REPO-LOCATION/.git/hooks. The cmd script assumes it is being run from the **GIT-REPO-LOCATION/format** directory
  
```
cd GIT-REPO-LOCATION/format
.\dotnet-format-pre-commit.cmd
```

After running this, when you go to commit, dotnet format should run automatically and correct any formatting that needs attention. 
