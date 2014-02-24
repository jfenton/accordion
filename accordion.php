<?php

/*
	Accord-ion (c) CloudGarage Asia 2013

	Uni-directional synchronisation of MySQL to Salesforce and/or Salesforce to MySQL.

	Usage:
		php accordion.php epoch
                                    |_ Epoch (seconds) to retrieve rows based on their Last Modified timestamp

	Author(s):
		Jay Fenton <jay.fenton@clougarage.asia>
	
*/

date_default_timezone_set('Hongkong');

function logger($indent, $msgs) {
	if($msgs != '') 
		foreach(explode(PHP_EOL, $msgs) as $msg)
			print date("d/m/Y H:i:s", time()).' - '.str_repeat("  ", $indent).$msg."\n";
}

/**
 * Retrieve the LastModified time from a given MySQL table
 *
 * @return string 
 */
function driverSalesforceGetLastModified($sf, $bac, $objName) {
	$soql = "SELECT LastModifiedDate FROM {$objName} ORDER BY LastModifiedDate DESC LIMIT 1";
	$response = $sf->query($soql);
	$result = $response->current()->LastModifiedDate;
	preg_match('/(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)\.\d+Z/', $result, $matches);
	$dt = new DateTime();
	$dt->setDate($matches[0], $matches[1], $matches[2]);
	$dt->setTime($matches[3], $matches[4], $matches[5]);
	return $dt;
}

/**
 * Returns number of entries that would be processed if we did not have a LIMIT predicate
 *
 * @return string 
 */
function driverSalesforceGetTotalRecords($sf, $bac, $objName, $lm, $predicate) {
	$ts = $lm->format("Y-m-d\TH:i:s.000\Z");
	// TODO: We should get all records that are >LM but also any with errors
	$soql = "SELECT COUNT() FROM {$objName} WHERE ";
	if($predicate) {
		$soql .= $predicate.' AND ';
	}
	$soql .= "LastModifiedDate > {$ts}";
	logger(3, 'SOQL:'.$soql);

	$response = $sf->query($soql);
	$result = $response->size;

	$totalrecords = intval($result);
	return $totalrecords;
}

/**
 * Utilises the Force.com Bulk API to perform SOQL queries, returning CSV output.
 *
 * @return string 
 */
function driverSalesforceGetData($sf, $bac, $objName, $fields, $lm, $predicate) {
	$selectFields = join(",", $fields);
	$ts = $lm->format("Y-m-d\TH:i:s.000\Z");
	// TODO: We should get all records that are >LM but also any with errors
	$soql = "SELECT {$selectFields} FROM {$objName} WHERE ";
	if($predicate) {
		$soql .= $predicate.' AND ';
	}
	$soql .= " LastModifiedDate > {$ts} ORDER BY LastModifiedDate DESC";
	logger(3, 'SOQL:'.$soql);
	// TODO: There is no LIMIT on Salesforce records. Adding one requires tracking bulk update status if no TS predicate is used.

	$job = new JobInfo();
	$job->setObject($objName);
	$job->setOperation("query");
	$job->setContentType("CSV");
	$job->setConcurrencyMode("Parallel");

	$job = $bac->createJob($job);
	$batch = $bac->createBatch($job, $soql);
	$bac->updateJobState($job->getId(), "Closed");

	logger(3, 'Waiting for query job completion...');
	while($batch->getState() == "Queued" || $batch->getState() == "InProgress") {
	    $batch = $bac->getBatchInfo($job->getId(), $batch->getId());
	    sleep(3);
	}

	if($batch->getState() == "Failed") {
		logger(4, "ERROR: ".$batch->getStateMessage());
	}

	logger(3, 'Job completed.');
	try {
		logger(3, 'Retrieving list of results...');
		$resultList = $bac->getBatchResultList($job->getId(), $batch->getId());
		logger(4, print_r($resultList, true));
		$resultId = $resultList[0];
		logger(3, 'Retrieving result...');
		$results = $bac->getBatchResult($job->getId(), $batch->getId(), $resultId);
//		print "== SOQL QUERY STRING == \n" . htmlspecialchars($soql) . "\n\n";
//		print "== QUERY RESULTS == \n" . print_r($results, true) . "\n\n";
//		print "== CLIENT LOGS == \n" . $bac->getLogs() . "\n\n";
	} catch(Exception $e) {
		if(0 === strpos($e->getMessage(), 'No result-list'))
			return '';
		else
			throw $e;
	}
	return $results;
}

/**
 * Retrieve the LastModified time from a given MySQL table
 *
 * @return string 
 */
function driverMySQLGetLastModified($link, $objName, $lmField) {
	$sql = "SELECT {$lmField} FROM {$objName} ORDER BY {$lmField} DESC LIMIT 1";
	logger(2, 'LastModified SQL: '.$sql);
	$result = mysqli_query($link, $sql);
	if(!$result) { logger(2, 'ERROR: '.mysqli_error($link)); }
	$row = mysqli_fetch_array($result);
	$last_modified = $row['update_time'];
	return date_create($last_modified);
}

/**
 * Returns number of entries that would be processed if we did not have a LIMIT predicate
 *
 * @return string 
 */
function driverMySQLGetTotalRecords($link, $objName, $lmField, $lm, $predicate) {
	$ts = $lm->format('Y-m-d H:i:s');
	// TODO: We should get all records that are >LM but also any with errors
	$sql = "SELECT COUNT(*) FROM {$objName} WHERE ";
	print $sql."\n";
	if($predicate != '') {
		$sql .= $predicate.' AND ';
	}
	$sql .= " Sync_Last IS NULL AND ";
	$sql .= "{$lmField} > '{$ts}' ORDER BY {$lmField} DESC";
	logger(3, 'Count SOQL:'.$sql);

	$result = mysqli_query($link, $sql);
	if(!$result) { logger(2, 'ERROR: '.mysqli_error($link)); }

	$row = mysqli_fetch_array($result);
	$totalrecords = intval($row[0]);
	logger(2, "There are {$totalrecords} records");
	return $totalrecords;
}

/**
 * Returns CSV output from a MySQL query
 *
 * @return string 
 */
function driverMySQLGetData($link, $objName, $fields, $lmField, $lm, $predicate) {
	$selectFields = join(",", $fields);
	$ts = $lm->format('Y-m-d H:i:s');
	// TODO: We should get all records that are >LM but also any with errors
	$sql = "SELECT {$selectFields} FROM {$objName} WHERE ";
	print $sql."\n";
	if($predicate != '') {
		$sql .= $predicate.' AND ';
	}
	$sql .= " Sync_Last IS NULL AND ";
	$sql .= " {$lmField} > '{$ts}' ORDER BY {$lmField} DESC LIMIT 10000";
	logger(3, 'SOQL:'.$sql);

	$results = array();

	$stmt = mysqli_query($link, $sql);
	if(!$stmt) { logger(2, 'ERROR: '.mysqli_error($link)); }
	$records = 0;
	while($result = mysqli_fetch_array($stmt, MYSQLI_ASSOC)) {
		// The dict contains two arrays - src (the original data), and dst (which will
		// be populated later, and become the transformed data, ready for insertion)
		array_push($results, array('src' => $result, 'dst' => NULL));
		$records++;
	}
	logger(2, "Retrieved {$records} records");
	return $results;
}

require_once('soapclient/SforcePartnerClient.php');
require_once('bulkapiclient/BulkApiClient.php');

if($argc < 2) { die("Syntax: php accordion.php config.ini lastmodifiedepoch\n"); }
logger(0, 'Accordion starting...');

/* Configuration Loader */
logger(0, 'Accordion loading configuration...');
$json_raw = file_get_contents($argv[1]);
if(!$json_raw) { die("Unable to open ".$json_raw."\n"); }

/* Strip comments from the configuration file, as native JSON decoder does not support comments */
$incomment = false;
$json_uncommented = '';
foreach(explode(PHP_EOL, $json_raw) as $line) {
	if($incomment) {
		if(0 === strpos(ltrim($line), "*/")) {
			// End multiline comment
			$incomment = false;
		}
	} else {
		if(0 === strpos(ltrim($line), "//")) {
			// Skip commented lines
			continue;
		}
		if(0 === strpos(ltrim($line), "/*")) {
			// Begin multiline comment
			$incomment = true;
		}
		if(!$incomment) {
			$json_uncommented .= $line;
		}
	}
}
	
/* Parse configuration */
$cfg = json_decode($json_uncommented);
if(!$cfg) { die("Error parsing accordion.cfg!\n"); }
$objs = $cfg->{'objects'};

define("USERNAME", $cfg->{'sf'}->{'username'});
define("PASSWORD", $cfg->{'sf'}->{'password'});
define("SECURITY_TOKEN", $cfg->{'sf'}->{'security_token'});

define("MYSQL_HOST", $cfg->{'db'}->{'host'});
define("MYSQL_USERNAME", $cfg->{'db'}->{'username'});
define("MYSQL_PASSWORD", $cfg->{'db'}->{'password'});
define("MYSQL_DATABASE", $cfg->{'db'}->{'database'});

/* MySQL Database Connection */
logger(0, 'Connecting to MySQL...');
$link = mysqli_connect(MYSQL_HOST, MYSQL_USERNAME, MYSQL_PASSWORD, MYSQL_DATABASE);
if(mysqli_connect_errno()) { die("Failed to connect to MySQL: ".mysqli_connect_error()."\n"); }

$stmt = mysqli_query($link, 'SET character_set_results=utf8;');
if(!$stmt) { logger(4, 'ERROR: '.mysqli_error($link)); }
$stmt = mysqli_query($link, 'set names "utf8";');
if(!$stmt) { logger(4, 'ERROR: '.mysqli_error($link)); }
$stmt = mysqli_query($link, 'SET character_set_client=utf8;');
if(!$stmt) { logger(4, 'ERROR: '.mysqli_error($link)); }
$stmt = mysqli_query($link, 'SET character_set_connection=utf8;');
if(!$stmt) { logger(4, 'ERROR: '.mysqli_error($link)); }
mb_language('uni'); 
mb_internal_encoding('UTF-8');

/* Force.com API Connection */
logger(0, 'Connecting to Salesforce...');
$sf = new SforcePartnerClient();
$sf->createConnection("partner.wsdl.xml");
$sf->login(USERNAME, PASSWORD.SECURITY_TOKEN);

logger(0, 'Establishing Bulk API connection...');
$bac = new BulkApiClient($sf->getLocation(), $sf->getSessionId());
$bac->setLoggingEnabled(true); //optional, but using here for demo purposes
$bac->setCompressionEnabled(true); //optional, but recommended. defaults to true.

/* Main Processing */
logger(0, 'Begin processing...');
foreach($objs as $objname => $obj) {
	logger(1, "Processing ".$obj->{'srcTable'});
	mysqli_autocommit($link, FALSE);

	// Get the Last Modified Date of our destination
	if($argv[1]) {
		$lm = new DateTime("@".$argv[1]);
		logger(2, "Last modified criteria: ".$lm->format("d/m/Y H:i:s"));
	} else if($obj->{'dst'} === "sf") {
		logger(2, "Retrieving last modified timestamp from Salesforce");
		$lm = driverSalesforceGetLastModified($sf, $bac, $obj->{'dstTable'});
		logger(2, "Salesforce last modified: ".$lm->format("d/m/Y H:i:s"));
	} else if($obj->{'dst'} === "db") {
		logger(2, "Retrieving last modified timestamp from MySQL");
		$lm = driverMySQLGetLastModified($link, $obj->{'dstTable'}, $obj->{'lastModifiedField'});
		logger(2, "MySQL table last modified: ".$lm->format("d/m/Y H:i:s"));
	}

	// Get the source data
	$selectFields = array();
	foreach($obj->{'fields'} as $field) {
		$fieldComponents = explode("->", $field);
		$srcField = $fieldComponents[0];
		if($srcField != NULL) {
			$matches = preg_match_all("/^(\d+|'.*')$/", $srcField, $srcFieldValue, PREG_SET_ORDER);
			logger(2, $matches);
			if($matches == 0) {
				array_push($selectFields, $obj->{'srcTable'}.'.'.$srcField);
			}
		}
	}

	$data = array();
	$errors = array();
	$totalrecords = 0;
	if($obj->{'src'} === "sf") {
		// Retrieve data from Salesforce
		logger(2, 'Retrieving '.$obj->{'srcTable'}.' from Salesforce');
		$totalrecords = driverSalesforceGetTotalRecords($sf, $bac, $obj->{'srcTable'}, $lm, array_key_exists('srcPredicate', $obj) ? $obj->{'srcPredicate'} : '');
		$dataRaw = driverSalesforceGetData($sf, $bac, $obj->{'srcTable'}, array_unique($selectFields), $lm, array_key_exists('srcPredicate', $obj) ? $obj->{'srcPredicate'} : '');
		$isFirstRow = true;
		$fieldsToNames = array();
		// Data is in CSV format - parse into dicts
		$records = 0;
		$multilineRowCatcher = NULL;
		foreach(explode(PHP_EOL, $dataRaw) as $row) {
			if($row == '') continue;
			$row = str_replace("\\", "\\\\", $row);
			if(substr($row, -strlen(1)) != '"') {
				$row = str_replace("\r", "\n", $row);
				$row = str_replace("\n", "\r", $row);
				if($multilineRowCatcher == NULL) {
					$multilineRowCatcher = $row;
				} else {
					$multilineRowCatcher = $multilineRowCatcher.', '.$row;
				}
				continue;
			}
			if($multilineRowCatcher != NULL) {
				$row = $multilineRowCatcher.', '.$row;
				$multilineRowCatcher = NULL;
			}

			logger(4, $row);
			$cols = str_getcsv($row);
			if($isFirstRow) {
				// If this is the first row, cache the field names from the CSV header
				$isFirstRow = false;
				foreach($cols as $col) {
					// Salesforce returns fields in column e.g. 'Account' when requesting field 'AccountId' - we transform it back here and strip the Id
					if(in_array($col.'Id', $selectFields)) {
						array_push($fieldsToNames, $col.'Id');
					} else {
						array_push($fieldsToNames, $col);
					}
				}
			} else {
				// Map each CSV column into the dict
				$entry = array();
				for($i = 0; $i < count($cols); $i++) {
					$entry[ $fieldsToNames[$i] ] = $cols[$i];
				}
				// The dict contains two arrays - src (the original data), and dst (which will
				// be populated later, and become the transformed data, ready for insertion)
				array_push($data, array('src' => $entry, 'dst' => NULL));
				$records++;
			}
		}
		logger(3, 'End of results');
		logger(2, "Retrieved {$records} records from Salesforce");
	} else if($obj->{'src'} === "db") {
		// Retrieve data from MySQL
		logger(1, 'Retrieving '.$obj->{'srcTable'}.' from MySQL');
		$totalrecords = driverMySQLGetTotalRecords($link, $obj->{'srcTable'}, $obj->{'lastModifiedField'}, $lm, array_key_exists('srcPredicate', $obj) ? $obj->{'srcPredicate'} : '');
		$data = driverMySQLGetData($link, $obj->{'srcTable'}, $selectFields, $obj->{'lastModifiedField'}, $lm, array_key_exists('srcPredicate', $obj) ? $obj->{'srcPredicate'} : '');
	}

	logger(2, "Total of {$totalrecords} at source");
	$numerrors = 0;

	if(count($data) == 0) {
		logger(2, 'No retrieved records at source.');
	} else {
		// For each field... (Note: we iterate over the configured fields in
		// the outer loop as an optimisation, as opposed to the inverse which
		// would require re-parsing the pipes and whatnot every time)
		logger(2, 'Marshaling fields...');
		foreach($obj->{'fields'} as $field) {
			logger(3, 'Marshaling field: '.$field);
			// ...determine how many pipes the field uses (a->b vs. a->b->c)
			$numpipes = substr_count($field, '->');
			if($numpipes == 2) {
				// Advanced - utilises an intermediate filter / processing function

				// Parse out the srcField->filter->dstField syntax into components
				preg_match_all("/^(.*)->(.*)->(.*)$/", $field, $pipes, PREG_SET_ORDER);
				$srcField = $pipes[0][1];
				$filter = $pipes[0][2];
				$dstField = $pipes[0][3];

				// Filters are of the form FUNCTION(ARGS), we break them out into components here
				preg_match_all("/^(\w+)\((.*)\)$/", $filter, $filterComponents, PREG_SET_ORDER);
				$filterCmd = $filterComponents[0][1];
				$filterArgsRaw = $filterComponents[0][2];
				$filterArgs = explode(",", $filterArgsRaw);

				// Filter logic
				if($filterCmd == 'LOOKUP') {
					// e.g. AccountId->LOOKUP(t_client,salesforce_id,client_id)->client_id
					//          |                  |         |            |         |______ The field to populate at the destination
					//          |                  |         |            |________________ The field to use for the value, if we find a match
					//	    |                  |         |_____________________________ The field to search at the destination
					//	    |                  |_______________________________________ The table to search at the destination
					//          |_________________________________________________________ The field at the source from which to get the search value
					$lookupTable = $filterArgs[0];
					$lookupUsing = $filterArgs[1];
					$thenUse = $filterArgs[2];

					$lookupFieldValues = array();
					foreach($data as &$datum) {
						// Add the field value we're looking for to an array, to be unique'd and passed to the IN operand
						$key = $datum['src'][$srcField];
						array_push($lookupFieldValues, '"'.$key.'"');
					}
					unset($datum);

					// Construct and prepare SQL statement (with placeholder values)
					$sql = "SELECT ".$lookupUsing.",".$thenUse." FROM ".$lookupTable." WHERE ".$lookupUsing." IN (".join(",", array_unique($lookupFieldValues)).")";
					logger(4, 'SQL:'.$sql);
					$stmt = mysqli_query($link, $sql);
					if(!$stmt) { logger(4, 'ERROR: '.mysqli_error($link)); }
					$map = array();
					while($result = mysqli_fetch_array($stmt, MYSQLI_ASSOC)) {
						$map[$result[$lookupUsing]] = $result[$thenUse];
					}
					
					foreach($data as &$datum) {
						if(array_key_exists($datum['src'][$srcField], $map)) {
							$datum['dst'][$dstField] = $map[ $datum['src'][$srcField] ];
						} else {
							logger(4, 'Unable to find a mapping for: ' + $datum['src'][$srcField]);
						}
					}
				} else if($filterCmd == 'SPLIT') {
					// e.g. Name->SPLIT(-1)->last_name
					//          |        |      |_____________ The field to populate at the destination
					//          |        |____________________ The index of the word (in this case the last word)
					//          |_____________________________ The field at the source from which to get the search value
					$index = intval($filterArgs[0]);
					foreach($data as &$datum) {
						$value = $datum['src'][$srcField];
						$components = explode(' ', $value);
						$element = array_slice($components, $index);
						$datum['dst'][$dstField] = $element[0];
						logger(4, 'SPLIT: '.$value.' to '.$element[0]);
					}
				}
			} else if($numpipes == 1) {
				$matches = preg_match_all("/^(\d+|'[^']+')->(.*)$/", $field, $srcField, PREG_SET_ORDER);
				if($matches == 1) {
					// Straight - mapping of field to a static value e.g. 42->life_universe or 'turtles'->all_way_down
					$srcFieldValue = $srcField[0][1];
					$dstField = $srcField[0][2];
					foreach($data as &$datum) {
						$datum['dst'][$dstField] = $srcFieldValue;
					}
				} else {
					// Straight - mapping of field to another field name
					preg_match_all("/^(.*)->(.*)$/", $field, $pipes, PREG_SET_ORDER);
					$srcField = $pipes[0][1];
					$dstField = $pipes[0][2];
					foreach($data as &$datum) {
						$datum['dst'][$dstField] = $datum['src'][$srcField];
					}
					unset($datum);
				}
			}
		}

		if($obj->{'dst'} === "db") {
			logger(2, 'Pushing to database...');

			$dstFields = array();
			$dstFieldPlaceholders = array();
			$dstFieldTypes = array();
			$dstFieldDuplicateKeyUpdate = array();
			// For each field...
			foreach($obj->{'fields'} as $field) {
				// ...construct arrays holding the field name, static placeholder, and field type in lock-step.
				preg_match_all("/^(.*)->(.*)(?:->|)(.*)$/", $field, $fieldComponents, PREG_SET_ORDER); // TODO: This works, but not sure why.. sorcery going on.
				$dstField = $fieldComponents[0][2];
				array_push($dstFields, $dstField);
				array_push($dstFieldPlaceholders, '?');
				array_push($dstFieldTypes, 's');
				array_push($dstFieldDuplicateKeyUpdate, $dstField." = values({$dstField})");
			}

			// Construct and prepare SQL statement (with placeholder values)
			$sql = "INSERT INTO ".$obj->{"dstTable"}."(".join(",", $dstFields).") VALUES (".join(',', $dstFieldPlaceholders).") ON DUPLICATE KEY UPDATE ".join(",", $dstFieldDuplicateKeyUpdate);
			logger(3, "INSERT SQL: {$sql}");
			$stmt = mysqli_prepare($link, $sql);
			if(!$stmt) { logger(2, 'ERROR: '.mysqli_error($link)); }

			// Create a string of all field types in order e.g. sssid (for string, string, string, integer, decimal)
			$dstPlaceholderTypes = join("", $dstFieldTypes);

			// For each data item...
			foreach($data as &$datum) {

				$fieldValues = array();
				// ...and each field on that data item...
				foreach($dstFields as $fieldName) {
					// ...add the field value to an array
					if(array_key_exists($fieldName, $datum['dst'])) {
						$value = $datum['dst'][$fieldName];
						array_push($fieldValues, $value);
					} else {
						array_push($fieldValues, '');
					}
				}

				// Convert the entries in the array to array references (as now required by call_user_func_array)
				$bindParams = array();
				foreach ($fieldValues as $key => $value) {
					$bindParams[$key] = &$fieldValues[$key]; 
				}

				// Call the mysql bind parameters with our assembled parameter array (called this way, because we need to pass a dynamic number of arguments)
				call_user_func_array('mysqli_stmt_bind_param', array_merge(array($stmt, $dstPlaceholderTypes), $bindParams));

				$erra = undef;
				if(!$stmt->execute()) {
					$erra = array($obj->{'srcPrimaryKey'} => $datum['src'][ $obj->{'srcPrimaryKey'} ], 'Has_Sync_Error__c' => true, 'Sync_Error_Message__c' => 'ERROR_MYSQL: '.mysqli_error($link)."\n".print_r($data[$i], true)."\n".date("d/m/Y H:i:s", time()));
					$numerrors++;
				} else {
					$erra = array($obj->{'srcPrimaryKey'} => $datum['src'][ $obj->{'srcPrimaryKey'} ], 'Has_Sync_Error' => false, 'Sync_Error_Message' => '');
				}
				if(array_key_exists('srcSecondaryKey', $obj)) {
					$erra[ $obj->{'srcSecondaryKey'} ] = $datum['src'][ $obj->{'srcSecondaryKey'} ];
				}
				array_push($errors, $erra);
			}
			unset($datum);
			//mysqli_commit($link);
			logger(2, 'Finished pushing to database');;
		} else if($obj->{'dst'} === "sf") {
			logger(2, 'Pushing to Salesforce...');
			$dstFields = array();
			// For each field...
			foreach($obj->{'fields'} as $field) {
				// ...construct arrays holding the field name, static placeholder, and field type in lock-step.
				preg_match_all("/^(.*)->(.*)(?:->|)(.*)$/", $field, $fieldComponents, PREG_SET_ORDER); // TODO: This works, but not sure why.. sorcery going on.
				$dstField = $fieldComponents[0][2];
				array_push($dstFields, $dstField);
			}

			$csvfh = fopen('php://temp/maxmemory:'. (256*1024*1024), 'r+'); // 256MB RAM, otherwise back to file

			fputcsv($csvfh, $dstFields);

			// For each data item...
			foreach($data as &$datum) {
				$fieldValues = array();
				// ...and each field on that data item...
				foreach($dstFields as $fieldName) {
					// ...add the field value to an array
					$value = $datum['dst'][$fieldName];
					array_push($fieldValues, $value);
				}

				fputcsv($csvfh, $fieldValues);
			}
			unset($datum);

			rewind($csvfh);
			$csv = stream_get_contents($csvfh);

			logger(2, 'Push to Salesforce');
			// logger(2, print_r($csv, true));

	/*
			$job = new JobInfo();
			$job->setObject($obj->{'dstTable'});
			$job->setOperation("upsert");
			$job->setContentType("CSV");
			$job->setConcurrencyMode("Parallel");                         //can also set to Serial
			$job->setExternalIdFieldName("Id");

			$job = $bac->createJob($job);
			$batch = $bac->createBatch($job, $csv);
			$bac->updateJobState($job->getId(), "Closed");

			logger(2, 'Awaiting results...');
			while($batch->getState() == "Queued" || $batch->getState() == "InProgress") {
			    $batch = $bac->getBatchInfo($job->getId(), $batch->getId());
			    sleep(3);
			}

			logger(2, 'Received results.');
			$batchResults = $bac->getBatchResults($job->getId(), $batch->getId());

			logger(2, $batchResults);
# DEBUG
#			logger(1, $bac->getLogs());

			// Process results
			$results = array();
			$isFirstRow = true;
			$fieldsToNames = array();
			foreach(explode(PHP_EOL, $batchResults) as $row) {
				if($row == '') continue;
				$cols = str_getcsv($row);
				if($isFirstRow) {
					// If this is the first row, cache the field names from the CSV header
					$isFirstRow = false;
					foreach($cols as $col) {
						array_push($fieldsToNames, $col);
					}
				} else {
					// Map each CSV column into the dict
					$entry = array();
					for($i = 0; $i < count($cols); $i++) {
						$entry[ $fieldsToNames[$i] ] = $cols[$i];
					}
					// The dict contains two arrays - src (the original data), and dst (which will
					// be populated later, and become the transformed data, ready for insertion)
					array_push($results, $entry);
				}
			}

			// Populate errors (and clear existing errors on success)
			$i = 0; // We need to run a counter as the Salesforce error results do not include the record Id, but are in the same order as the request rows.
			foreach($results as $entry) {
				$erra = null;
				if($entry['Success'] == 'false') {
//					logger(2, print_r($data[$i], true));
					$erra = array($obj->{'srcPrimaryKey'} => $data[$i]['src'][ $obj->{'srcPrimaryKey'} ], 'Has_Sync_Error' => true, 'Sync_Error_Message' => 'ERROR_SALESFORCE: '.$entry['Error']."\n".print_r($data[$i], true)."\n".date("d/m/Y H:i:s", time()), 'salesforce_id' => $entry['Id']);
					$numerrors++;
				} else {
					$erra = array($obj->{'srcPrimaryKey'} => $data[$i]['src'][ $obj->{'srcPrimaryKey'} ], 'Has_Sync_Error' => false, 'Sync_Error_Message' => '', 'salesforce_id' => $entry['Id']);
				}
				if(array_key_exists('srcSecondaryKey', $obj)) {
					$erra[ $obj->{'srcSecondaryKey'} ] = $data[$i]['src'][ $obj->{'srcSecondaryKey'} ];
				}
				array_push($errors, $erra);
				$i++;
			}
		}
	}

	logger(3, print_r($errors, true));

	if(count($data) == 0) {
		// NOOP
	} else if($obj->{'src'} === "sf") {
		logger(2, "Uploading {$numerrors} errors to Salesforce");

		$csvfh = fopen('php://temp/maxmemory:'. (256*1024*1024), 'r+'); // 256MB RAM, otherwise back to file
		fputcsv($csvfh, array('Id', 'Has_Sync_Error__c', 'Sync_Error_Message__c'));
		foreach($errors as $error) {
			fputcsv($csvfh, $error);
		}

		rewind($csvfh);
		$csv = stream_get_contents($csvfh);

		logger(4, print_r($csv, true));

		$job = new JobInfo();
		if($obj->{'src'} === "sf") {
			$job->setObject($obj->{'srcTable'});
		} else if($obj->{'dst'} === "sf") {
			$job->setObject($obj->{'dstTable'});
		}
		$job->setOperation("upsert");
		$job->setContentType("CSV");
		$job->setConcurrencyMode("Parallel");
		$job->setExternalIdFieldName("Id");

		$job = $bac->createJob($job);
		$batch = $bac->createBatch($job, $csv);
		$bac->updateJobState($job->getId(), "Closed");

		logger(2, 'Waiting for error upload job completion...');
		while($batch->getState() == "Queued" || $batch->getState() == "InProgress") {
		    $batch = $bac->getBatchInfo($job->getId(), $batch->getId());
		    sleep(3);
		}

		logger(2, 'Job completed.');
		$batchResults = $bac->getBatchResults($job->getId(), $batch->getId());
		// logger(2, 'Error upload results');
		// logger(3, $batchResults);
		logger(2, 'Error upload completed.');

		logger(1, "Finished processing ".$obj->{'srcTable'});
	} else if($obj->{'src'} == "db") {
		logger(2, "Uploading {$numerrors} errors to Database");

		// Construct and prepare SQL statement (with placeholder values)
		$sql = "UPDATE ".$obj->{"srcTable"}." SET Has_Sync_Error=?,Sync_Error_Message=?,Sync_Last=NOW(),salesforce_id=? WHERE ".$obj->{'srcPrimaryKey'}."=?";
		if(array_key_exists('srcSecondaryKey', $obj)) {
			$sql .= ' AND '.$obj->{'srcSecondaryKey'}.'=?';
		}
		logger(3, "UPDATE SQL: {$sql}");
		$stmt = mysqli_prepare($link, $sql);
		if(!$stmt) { logger(2, 'ERROR: '.mysqli_error($link)); }

		// For each data item...
		foreach($errors as &$error) {
			$fieldValues = array();
			array_push($fieldValues, $error['Has_Sync_Error']);
			array_push($fieldValues, $error['Sync_Error_Message']);
			array_push($fieldValues, $error['salesforce_id']); // TODO: This will overwrite salesforce_id with NULL on the DB in cases where an SF error is thrown on an upsert for an existing record
			array_push($fieldValues, $error[ $obj->{'srcPrimaryKey'} ]);
			if(array_key_exists('srcSecondaryKey', $obj)) {
				array_push($fieldValues, $error[ $obj->{'srcSecondaryKey'} ]);
			}

			// Convert the entries in the array to array references (as now required by call_user_func_array)
			$bindParams = array();
			foreach ($fieldValues as $key => $value) {
				$bindParams[$key] = &$fieldValues[$key]; 
			}

			logger(3, print_r($bindParams, true));

			// Call the mysql bind parameters with our assembled parameter array (called this way, because we need to pass a dynamic number of arguments)
			
			if(array_key_exists('srcSecondaryKey', $obj)) {
				call_user_func_array('mysqli_stmt_bind_param', array_merge(array($stmt, 'issss'), $bindParams));
			} else {
				call_user_func_array('mysqli_stmt_bind_param', array_merge(array($stmt, 'isss'), $bindParams));
			}

			if(!$stmt->execute()) {
				logger(4, 'ERROR_MYSQL: '.mysqli_error($link));
			}
		}
		unset($datum);
		//mysqli_commit($link);
	}

	if($obj->{'src'} == "db") {
		$remainingrecords = $totalrecords - count($data);
		logger(2, "There are {$remainingrecords} remaining");
		array_push($retvalues, $remainingrecords);
	} else {
		array_push($retvalues, 0);
	}

	mysqli_commit($link);
}
logger(0, 'Finished processing.');

//logger(0, print_r($data, true));

//logger(0, print_r($errors, true));

$bac->clearLogs(); // Clear logging buffer

// Exit with return code 0 = there are outstanding records, 1 = no further records to process
exit(max($retvalues) > 0 ? 0 : 1);

?>
