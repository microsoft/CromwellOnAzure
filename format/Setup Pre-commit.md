# Set up Pre-commit
To set up dotnet format to run locally on commit, you need to perform the following actions. You should only need to do this once unless you remove and clone the repo again. 

## Git pre-commit hook to reformat
Navigate to <repolocation>/format. Run the "dotnet-format-pre-commit.cmd" to copy the pre-commit file into the .git/hooks directory. Verify that the file has been copied as expeted into <repolocation>/.git/hooks. 

For now on, when you go to commit, dotnet format should run automatically and correct any formatting that needs attention. 