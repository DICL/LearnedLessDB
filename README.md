# LearnedLessDB

HyperWiscKey
HyperBourbon(CBA)
HyperBourbon(Always)

Install
=======

Get up and running quickly:

    $ autoreconf -i
    $ ./configure
    $ make
    # make install
    # ldconfig


Run YCSB
========

    $ source ready_ycsb.sh
    $ ./run_ycsb


Run SOSD
========

    $ vi koo/koo.h --> YCSB_SOSD 1
    $ source ready_ycsb.sh
    $ ./run_sosd
