#!/bin/bash
T=`date +'%s'`
echo $T >accordion.last
php accordion.php $T
