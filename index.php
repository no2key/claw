<?php
	//注入问题暂时不解决:-)
	$domain = $_GET['domain'];
	$dbName = str_replace('.','_',$domain);
	$con = mysql_connect("localhost", "root", "mysql");

	if(!$con)
	{
	  die('Could not connect: ' . mysql_error());
	}

	mysql_select_db('claw',$con);
	$sql = "SELECT * from $dbName";
	$result = mysql_query($sql,$con);

	echo "<h1>$domain</h1>";

	while($row = mysql_fetch_assoc($result))
	{
		echo $row['update_date'].': ';
		echo $row['uri'].'<br />';
	}

	mysql_close($con);
?>
