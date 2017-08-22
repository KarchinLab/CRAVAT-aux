use LWP;
use URI;

my $browser = LWP::UserAgent->new();

print "##### Job Status #####\n";
my $urlStatus = URI->new('http://www.cravat.us/rest/service/status');
$urlStatus->query_form('jobid' => 'rkim_20140430_110111');
my $responseStatus = $browser->get($urlStatus);
print $responseStatus->content, "\n";

print "\n###### Submitting through GET #####\n";
my $urlSubmitGet = URI->new("http://www.cravat.us/rest/service/submit");
$urlSubmitGet->query_form(
        'analyses' => 'CHASM',
        'analysistype' => 'driver',
        'chasmclassifier' => 'Ovary',
        'email' => 'rkim@insilico.us.com',
        'functionalannotation' => 'off',
        'hg18' => 'off',
        'mutations' => 'TR1 chr22 30421786 + A T',
        'mupitinput' => 'off',
        'tsvreport' => 'off');
my $responseSubmitGet = $browser->get($urlSubmitGet);
print $responseSubmitGet->content, "\n";

print "\n###### Submitting through POST #####\n";
my $urlSubmitPost = "http://www.cravat.us/rest/service/submit";
my $responseSubmitPost = $browser->post($urlSubmitPost,
        Content_Type => 'form-data',
        Content => [
        'analyses' => 'CHASM',
        'analysistype' => 'driver',
        'chasmclassifier' => 'Ovary',
        'email' => 'rkim@insilico.us.com',
        'functionalannotation' => 'off',
        'hg18' => 'off',
        'mupitinput' => 'off',
        'tsvreport' => 'off',
        'inputfile' => ['/home/rkim/5muts.txt']]);
print $responseSubmitPost->headers()->as_string;
print $responseSubmitPost->content, "\n";
