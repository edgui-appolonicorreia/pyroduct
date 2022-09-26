# Use rm -f before compile the lambdas so it will re-create the zip
# in case it alread exists

# extraction
cd oracle_extraction/
rm -f function.zip
zip function.zip lamb_function.py
cd ..

# transfrom
cd transform/
rm -f function.zip
zip function.zip lamb_function.py
cd ..

# load
cd load/
rm -f function.zip
zip function.zip lamb_function.py
cd ..
