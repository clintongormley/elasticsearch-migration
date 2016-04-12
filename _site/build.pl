#!/usr/bin/env perl

use strict;
use warnings;
use File::Slurp qw(slurp);
use File::Basename qw(basename dirname);

my $file = shift @ARGV || die <<"USAGE";
    $0 file_to_compile.js > compiled.js
USAGE

my $dir = dirname($file);
chdir $dir;

sub process_file {
    my $file = shift();
    my @lines = split /\n/, slurp($file);
    my @out;
    while (@lines) {
        my $line = shift @lines;
        if ( $line =~ m/^\s*require\(['"]([^'"]+)['"]\)/ ) {
            push @out, process_file($1);
        }
        else {
            push @out, $line;
        }
    }
    return @out;
}

print join "\n", process_file( basename $file);

