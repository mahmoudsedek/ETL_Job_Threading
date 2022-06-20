#! /bin/bash

# run when you're inside the shell_script directory

cd ../error

# move the files back to input directory
mv -v ./identity/* ../input/identity/
mv -v ./right_to_work/* ../input/checks/right_to_work/

# delete the 2 directories
rm -d identity/
rm -d right_to_work/