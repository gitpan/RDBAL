package RDBAL::Config;

# Directory to use for cache directory
$cache_directory = '/usr/local/schema_cache';

# Preferred order of middle layer drivers
@search_order = ('ApacheSybaseDBlib',
		 'SybaseDBlib',
		 'ODBC',
		 'dbi:Sybase',
		 'dbi:Oracle',
		 'dbi:mSQL'
		 );
# Perl module for each middle layer driver
%middle_module = ('ApacheSybaseDBlib' => 'Apache::Sybase::DBlib',
		  'SybaseDBlib' => 'Sybase::DBlib',
		  'ODBC' => 'Win32::ODBC',
		  'dbi:' => 'DBI'
		  );
