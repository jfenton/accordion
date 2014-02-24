#!/bin/bash

configs=( Client Client_Contact Vendor Vendor_Contact Brand Employees Quotation Quotation_Item Project Project_Brief Purchase_Order Client_Invoice )

for cfg in "${configs[@]}"
do
	LOCKFILE=/tmp/accordion.$cfg.lock
	TIMEFILE=/tmp/accordion.$cfg.last
	if [ -e ${LOCKFILE} ] && kill -0 `cat ${LOCKFILE}`; then
		echo "Accordion is already running for $cfg based on existence of $LOCKFILE. Refusing to run again."
	else
		echo -n >accordion.log.$cfg

		trap "rm -f ${LOCKFILE}; exit" INT TERM EXIT
		echo $$ > ${LOCKFILE}

		if [ -e ${TIMEFILE} ]; then
			T=`cat $TIMEFILE`
			T="${T%\\n}" # Remove trailing CR
		else
			T=1
		fi

		NT=`date -d @$T`

		while [ $? -eq 0 ]; do
			echo "Accordion exec for $cfg from $NT..."
			php accordion.php accordion.cfg.$cfg $T >>accordion.log.$cfg 2>&1
		done

		date +'%s' >$TIMEFILE

		rm -f ${LOCKFILE}
		trap - INT TERM EXIT
	fi
done

