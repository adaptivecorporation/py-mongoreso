# py-mongoreso

The (better) version of py-resoimpoter. This method is much more effective and is a simpler loop. Note this does not download the images. In the version 1 RESO API (MLS Grid), images are queried separately. This script doesn't include the method to download the images.

The best way to achieve this is to create an array of the MediaURLs and use multithreading to download the files and store them in a place of your choosing.