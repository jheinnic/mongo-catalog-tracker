#!/usr/bin/perl


sub calculate_use {
	my $dbName = shift;
	my $targetFile = shift;
	my $outputFile = shift;
	open(INPUT, $targetFile);
	open(RAWRECORDS, ">${outputFile}");
	print RAWRECORDS "VariomeName|IndexName|UseCount|Since\n";
	my %collections = {};
	my %indices = {};

	my @readStates = ['expect_collection', 'expect_index', 'expect_index_or_collection', 'expect_opcount'];
	my $readState = 'expect_collection';
	my $trackedVariome = {};
	my $lineCount = 0;
	my $inputLine = <INPUT>;

	while($inputLine = <INPUT>) {
		chomp $inputLine;
		# print("## ${inputLine} ##\n");
		$lineCount = $lineCount + 1;
		if ($readState eq 'expect_collection') {
			if ($inputLine =~ /^(${dbName}.*):$/) {
				$collectionName = $1;
				$readState = 'expect_index';
			} elsif (($inputLine =~ /simagix\/keyhole/) or ($inputLine =~ /I GetIndexes ends/)) { 
				# No-op for verbosity preamble/postinfo
			} else {
				print "Compilation error!  Expected collection name line, but read <${inputLine}> at <${lineCount}>\n";
				exit -1;
			}
		} elsif ($readState eq 'expect_index_or_collection') {
			if ($inputLine =~ /^(${dbName}.*):$/) {
				$collectionName = $1;
				$indexName = '';
				$readState = 'expect_index';
			} elsif ($inputLine =~ /^{.*}$/) {
				$indexName = $inputLine;
				$readState = 'expect_opcount';
			} elsif ($inputLine =~ /is larger than the max int32/) {
				# No-op for verbosity
			} else {
				print "Compilation error!  Expected collection or index line, but read <${inputLine}> at <${lineCount}>\n";
				exit -1;
			}
		} elsif ($readState eq 'expect_index') {
			if ($inputLine =~ /^{.*}$/) {
				$indexName = $inputLine;
				$readState = 'expect_opcount';
			} else {
				print "Compilation error!  Expected index line, but read <${inputLine}> at <${lineCount}>\n";
				exit -1;
			}
		} elsif ($readState eq 'expect_opcount') {
			if ($inputLine =~ /ops: ([0-9]+), since: (.* UTC)/) {
				my $opCount = $1;
	   			my $since = $2;
				print RAWRECORDS "${collectionName}|${indexName}|${opCount}|${since}\n";
				$indexName = '';
				$readState = 'expect_index_or_collection';
			} else {
				print "Compilation error!  Expected op count line, but read <${inputLine}> at <${lineCount}>\n";
				exit -1;
			}
		} else {
			print "Unknown read state?  <${readState}> at <${lineCount}>\n";
			exit -1;
		}
	}

	close RAWRECORDS;
	close INPUT;
}

my $dbName = $ARGV[0];
my $fileArg = $ARGV[1];
my $outputArg = $ARGV[2];
calculate_use(${dbName}, ${fileArg}, ${outputArg});
