param([string]$testName="blank")
if($testName -eq "blank"){
	$testName = read-host "Enter a test name"}
$xml_text = "<desc>
	<sub_params>
		<analyses />
	</sub_params>
	<verify_rules />
	<tab>variants</tab>
</desc>"
$files = "desc.xml", "input.txt", "key.csv"

if(test-path ./$testName ){
	$continue = read-host "Directory exists. Enter <y> to overwrite"
	if($continue -ne ("y")){exit}
	remove-item -recurse $testName
}
new-item $testName -type directory

foreach($name in $files){
	$fullPath = "$testName/$testName"+"_$name"
	echo $fullPath
	new-item $fullPath
	if ($name -eq "desc.xml"){
		add-content $fullPath $xml_text
	}
}


