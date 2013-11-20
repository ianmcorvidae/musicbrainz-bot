#!/usr/bin/perl
# perl bot.pl [options]

use strict;
use warnings;

use FindBin;
use lib "$FindBin::Bin";
use utf8;
use Data::Dumper; 
use DBI;
use Getopt::Long;
use MusicBrainzBot;
use JSON;
use LWP::Simple;
use POSIX qw(strftime);

use LWP::UserAgent;
my $ua = LWP::UserAgent->new;
$ua->agent("area_bot/0.1 geonames.pl");

binmode STDOUT, ":utf8";

my $username = "area_bot";
my $password = "";
my $server = "beta.musicbrainz.org";
my $db = "musicbrainz_db_slave";
my $protocol = 'https://';
# For testing
#$server = "ianmcorvidae.mbsandbox.org";
#$db = "musicbrainz_db_static";
#$protocol = 'http://';
my $verbose = 1;
my $max = 10000;
my $dryrun = 0;

my $geonameslt = 713;

open SETTINGS, "area_bot.json" or die;
my $settingsj = <SETTINGS>;
close SETTINGS;
my $settings = decode_json($settingsj);
$password = $settings->{"password_".$server};

my $dbh = DBI->connect("dbi:Pg:dbname=$db", 'musicbrainz', '', { pg_enable_utf8 => 1 }) or die;
$dbh->do("SET search_path TO musicbrainz, geonames, public");

my $geonames_urls = {};

my $query = "
WITH normalized_alt AS
    (SELECT alternatenameid, alternatename.geonameid, (CASE WHEN alternatename ~ '%[0-9A-F][0-9A-F]' THEN uri_decode_encode(alternatename) ELSE alternatename END) as alternatename
     FROM geonames.alternatename JOIN geonames.geoname ON alternatename.geonameid = geoname.geonameid
     WHERE isolanguage = 'link' AND alternatename ~ 'wikipedia.org' AND geoname.fclass = any(ARRAY['A', 'P'])),
     existing_geonames AS
    (SELECT array_agg(regexp_replace(url, 'http://sws.geonames.org/([0-9]+)/', E'\\\\1')::integer) AS geonameids, l_area_url.entity0 AS area
     FROM url JOIN l_area_url ON l_area_url.entity1 = url.id JOIN link ON l_area_url.link = link.id
     WHERE link.link_type = ? GROUP BY l_area_url.entity0)
SELECT url, normalized_alt.geonameid, area.gid as area, existing_geonames.geonameids AS old_geonames
FROM musicbrainz.url
join normalized_alt on url.url = normalized_alt.alternatename
join l_area_url on l_area_url.entity1 = url.id left
join existing_geonames ON l_area_url.entity0 = existing_geonames.area JOIN area on l_area_url.entity0 = area.id
WHERE (normalized_alt.geonameid != all(existing_geonames.geonameids) OR existing_geonames.geonameids IS NULL)
AND NOT EXISTS (SELECT TRUE FROM existing_geonames eg2 WHERE normalized_alt.geonameid = any(existing_geonames.geonameids))
ORDER BY existing_geonames.geonameids NULLS FIRST, area.type ASC LIMIT $max
";

my $sthc = $dbh->prepare($query) or die $dbh->errstr;
$sthc->execute($geonameslt);
while (my ($url, $geoname, $area, $old_geonames) = $sthc->fetchrow()) {
    $geonames_urls->{$area} ||= [];
    push @{ $geonames_urls->{$area} }, {geoname => $geoname, via => $url, change_from => $old_geonames};
}

my $bot = MusicBrainzBot->new({ username => $username, password => $password, server => $server, verbose => $verbose, protocol => $protocol });
for my $area (keys %$geonames_urls) {
    for my $item (@{ $geonames_urls->{$area} }) {
        my $geoname = $item->{geoname};
        my $url = $item->{via};
        my @change_from = @{ $item->{change_from} // [] };
        #if (!defined $change_from) {
            print STDERR "Adding geonames id $geoname to area $area via shared url $url. (previously: @change_from)\n";
            my $rv = $bot->add_url_relationship($area, "area", {
                'link_type_id' => $geonameslt,
                url => "http://sws.geonames.org/$geoname/",
                "as_auto_editor" => 1,
                edit_note => "Connected to geonames via shared wikipedia URL $url."
            });
        #} else {
        #    print STDERR "Changing geonames id $change_from to $geoname, linked to area $area. UNIMPLEMENTED.\n";
        #}
    }
}
