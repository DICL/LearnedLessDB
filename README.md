# LearnedLessDB


Install
=======

    $ autoreconf -i
    $ ./configure
    $ make
    # make install


Run YCSB
========

    $ source ready_ycsb.sh
    $ ./run_ycsb			# Put db_path


Run SOSD
========

    $ source ready_ycsb.sh
    $ ./run_sosd			# Put db_path, sosd_data_path, sosd_lookups_path
