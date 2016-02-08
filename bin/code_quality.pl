#!/usr/bin/perl

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

use strict;
displayCheckStyleWarnings("plugins/idea/pluginBase/target/checkstyle-result.xml");
displayCheckStyleWarnings("plugins/idea/pluginImpl/target/checkstyle-result.xml");
displayFindBugsWarnings("plugins/idea/pluginBase/target/findbugs/report.xml");
displayFindBugsWarnings("plugins/idea/pluginImpl/target/findbugs/report.xml");
displayPMDWarnings("plugins/idea/pluginBase/target/pmd.xml");
displayPMDWarnings("plugins/idea/pluginImpl/target/pmd.xml");

# Findbugs
sub displayFindBugsWarnings {

open FILE, $_[0] or die "Couldn't open file: $!";
my $string = "";
my $count = -1;
my $indx = index($_[0], "/target");
my $proj = substr($_[0], 0, $indx);
while (<FILE>) {
    $string .= $_;
    if(/<Errors errors=\"(\d+).*/) {
        $count = $1;
    }
}
if ($count > 0) {
    print "ERROR: Found findbug violations in ${proj}\n";
    print $string;

}
print "SUCCESS: No findbug violations\n";
close FILE;
}

# Checkstyle
sub displayCheckStyleWarnings {
open FILE, $_[0] or die "Couldn't open file: $!";
my $string = "";
my $count = 0;
my $indx = index($_[0], "/target");
my $proj = substr($_[0], 0, $indx);
while (<FILE>) {

    if(/<file .*/) {
        $string .= $_;
    }

    if(/<error .*/) {
        $string .= $_;
        $count = $count + 1;
    }
}

if ($count > 0) {
    print "ERROR: Found checkstyle violations in ${proj}\n";
    print $string;

}
print "SUCCESS: No checkstyle violations\n";
close FILE;
}

# PMD
sub displayPMDWarnings {
open FILE, $_[0] or die "Couldn't open file: $!";

my $string = "";
my $count = 0;
my $indx = index($_[0], "/target");
my $proj = substr($_[0], 0, $indx);

while (<FILE>) {

    if(/<file .*/) {
        $string .= $_;
    }

    if(/<violation .*/) {
        $string .= $_;
        $count = $count + 1;
    }
}

  if ($count > 4 && index($proj , "pluginBase") != -1) { # allowing 4 violations only if module is pluginBase
     print "ERROR: Found additional PMD violations in ${proj}\n";
     print $string;
  } elsif ($count > 4 && index($proj , "pluginBase") == -1 ) {
     print "ERROR: Found PMD violations in ${proj}\n";
     print $string;
  } else {
     print "SUCCESS: No PMD violations\n";
     close FILE;
  }
}
