# MapleJuice Project

Welcome to the MapleJuice Project!

## Demo Script

```console

# 0. Load data
sdfs put-dir -dir demo

# 1. Percent Composition
maple -maple_exe maple1.py -num_maples 10 -sdfs_intermediate_filename_prefix fiber-int1 -sdfs_src_directory demo -partition_type r -maple_cmd_args '-interconne Fiber'
juice -juice_exe juice1.py -num_juices 10 -sdfs_intermediate_filename_prefix fiber-int1 -sdfs_dest_filename fiber-out1 -partition_type r --delete_input
maple -maple_exe maple2.py -num_maples 10 -sdfs_intermediate_filename_prefix fiber-int2 -sdfs_src_directory fiber-out1 -partition_type r
juice -juice_exe juice2.py -num_juices 10 -sdfs_intermediate_filename_prefix fiber-int2 -sdfs_dest_filename fiber-out2 -partition_type r --delete_input

maple -maple_exe maple1.py -num_maples 10 -sdfs_intermediate_filename_prefix fiber_radio-int1 -sdfs_src_directory demo -partition_type r -maple_cmd_args '-interconne Fiber/Radio'
juice -juice_exe juice1.py -num_juices 10 -sdfs_intermediate_filename_prefix fiber_radio-int1 -sdfs_dest_filename fiber_radio-out1 -partition_type r --delete_input
maple -maple_exe maple2.py -num_maples 10 -sdfs_intermediate_filename_prefix fiber_radio-int2 -sdfs_src_directory fiber_radio-out1 -partition_type r
juice -juice_exe juice2.py -num_juices 10 -sdfs_intermediate_filename_prefix fiber_radio-int2 -sdfs_dest_filename fiber_radio-out2 -partition_type r --delete_input

maple -maple_exe maple1.py -num_maples 10 -sdfs_intermediate_filename_prefix radio-int1 -sdfs_src_directory demo -partition_type r -maple_cmd_args '-interconne Radio'
juice -juice_exe juice1.py -num_juices 10 -sdfs_intermediate_filename_prefix radio-int1 -sdfs_dest_filename radio-out1 -partition_type r --delete_input
maple -maple_exe maple2.py -num_maples 10 -sdfs_intermediate_filename_prefix radio-int2 -sdfs_src_directory radio-out1 -partition_type r
juice -juice_exe juice2.py -num_juices 10 -sdfs_intermediate_filename_prefix radio-int2 -sdfs_dest_filename radio-out2 -partition_type r --delete_input

maple -maple_exe maple1.py -num_maples 10 -sdfs_intermediate_filename_prefix None-int1 -sdfs_src_directory demo -partition_type r -maple_cmd_args '-interconne None'
juice -juice_exe juice1.py -num_juices 10 -sdfs_intermediate_filename_prefix None-int1 -sdfs_dest_filename None-out1 -partition_type r --delete_input
maple -maple_exe maple2.py -num_maples 10 -sdfs_intermediate_filename_prefix None-int2 -sdfs_src_directory None-out1 -partition_type r
juice -juice_exe juice2.py -num_juices 10 -sdfs_intermediate_filename_prefix None-int2 -sdfs_dest_filename None-out2 -partition_type r --delete_input

maple -maple_exe maple1.py -num_maples 10 -sdfs_intermediate_filename_prefix BLANK-int1 -sdfs_src_directory demo -partition_type r -maple_cmd_args '-interconne (BLANK)'
juice -juice_exe juice1.py -num_juices 10 -sdfs_intermediate_filename_prefix BLANK-int1 -sdfs_dest_filename BLANK-out1 -partition_type r --delete_input
maple -maple_exe maple2.py -num_maples 10 -sdfs_intermediate_filename_prefix BLANK-int2 -sdfs_src_directory BLANK-out1 -partition_type r
juice -juice_exe juice2.py -num_juices 10 -sdfs_intermediate_filename_prefix BLANK-int2 -sdfs_dest_filename BLANK-out2 -partition_type r --delete_input

maple -maple_exe maple1.py -num_maples 10 -sdfs_intermediate_filename_prefix SPACE-int1 -sdfs_src_directory demo -partition_type r -maple_cmd_args '-interconne " "'
juice -juice_exe juice1.py -num_juices 10 -sdfs_intermediate_filename_prefix SPACE-int1 -sdfs_dest_filename SPACE-out1 -partition_type r --delete_input
maple -maple_exe maple2.py -num_maples 10 -sdfs_intermediate_filename_prefix SPACE-int2 -sdfs_src_directory SPACE-out1 -partition_type r
juice -juice_exe juice2.py -num_juices 10 -sdfs_intermediate_filename_prefix SPACE-int2 -sdfs_dest_filename SPACE-out2 -partition_type r --delete_input


# Get results
sdfs get -sdfs_filename fiber-out2 -local_filename fiber-out2-result.txt
sdfs get -sdfs_filename fiber_radio-out2 -local_filename fiber_radio-out2.txt
sdfs get -sdfs_filename radio-out2 -local_filename radio-out2-result.txt
sdfs get -sdfs_filename None-out2 -local_filename None-out2-result.txt
sdfs get -sdfs_filename BLANK-out2 -local_filename BLANK-out2-result.txt


# 2. SQL Filter (Keyword Search)
sql sql/filter.sqlx -sdfs_dest_filename filter-result.txt -partition_type r
sdfs get -sdfs_filename champaign.txt -local_filename filter-result.txt
```

## Report
```console

# 0. Load data
sdfs put-dir -dir ../data

# 1. Simple Query
sql-filter Address_Points "^Lakeshore$" -sdfs_dest_filename address_data.txt -partition_type r
sdfs get -local_filename lakeshore-points.txt -sdfs_filename address_data.txt

# 2. Complex Query
sql-filter Address_Points "^\d{3}.*\d{5}$" -sdfs_dest_filename complex1.txt -partition_type r
sdfs get -sdfs_filename complex.txt -local_filename complex-results.txt

# 3. Simple SQL Joins
sdfs put-dir -dir ../pokemon
sql-join sql/pokemon.sql -sdfs_dest_filename pokemon -partition_type r 

sdfs put-dir -dir ../names
sql-join sql/names.sql -sdfs_dest_filename names -partition_type r 
```


## Quickstart

```bash
# 0. Ensure you have python 3.6.8 or higher
python3 --version

# 1. Clone
git clone https://github.com/your-username/MapleJuice.git
cd MapleJuice

# 2. Setup
pip install .

# 3. Activate MapleJuice
maplejuice

# Simple Distributed File System (SDFS) Command Cheatsheet
#
# Create and update files (base_dir is a relative directory the script was invoked in)
sdfs put -local_filename bible.txt -sdfs_filename foo.txt -base_dir ../large.txt

# List all files the Name Node knows about (defaults to fa23-cs425-7810.cs.illinois.edu)
#
sdfs ls

# List files stored on a data node (choices=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])
#
sdfs store -dn 1

# Retrieve a file from the SDFS
#
sdfs get -sdfs_filename illiad.txt -local_filename result.txt
#
# Delete the file from the SDFS
#
sdfs delete -sdfs_filename foo.txt
#
# Membership Failure Detection Command Cheatsheet
#
# Activates a CLI menu program to send admin commands to the underlying membership detection service
membership
```
