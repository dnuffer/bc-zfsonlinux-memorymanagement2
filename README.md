
available from:
    http://brockmann-consult.de/peter2/zfsonlinux_memorymanagement2.tgz

It is intended to run on a storage machine like a NAS, so the defaults are to try to use all RAM. Maybe when sharing wtih other programs, this means it gives up memory too slowly. Use the command line arguments to adjust.

Run it in cron every minute or whatever you think is safe. I recommend redirecting to /dev/null, or saving the log somewhere and using logrotate. (TODO: support rotation properly. Right now you have to use copytruncate, or kill the python process when you rotate and cron will start it again).

It will loop forever and keep tuning and dropping caches if used RAM gets too high.

Both of my large 36 disk box ZoL machines hang if I don't run this. I wrote this in bash years ago and recently redid it in python3 to fix the float handling and exceptions.

The bc_zfsonlinux_memorymanagement2.py symlink exists for including as a module. Python doesn't like minus signs in module names, but I don't like underscore in command names.
