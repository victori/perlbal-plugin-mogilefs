package Perlbal::Plugin::MogileFS;

use Perlbal;
use strict;
use warnings;
use Data::Dumper;
use MogileFS::Client;

#
# LOAD MogileFS
# SET plugins        = MogileFS

# define all stats keys here
our @statkeys = qw(mogilefs_requests mogilefs_misses mogilefs_hits);

# modify this function to adapt urls to your mogilefs keys
sub url_to_key {
  my $uri = shift;
  if($uri =~ /\/thumbx[0-9]+\//) {
    my ($type,$id,$size,$pic) = $uri =~ /\/(.*)\/([0-9]+)\/thumbx([0-9]+)\/(.*)/;
    return "$type:$id:$pic:$size";
  } else {
    return join(':',split(/\//,substr($uri,1))); 
  }
}

sub mogilefs_config {
  my $mc = shift->parse(qr/^mogilefs\s*(domain|trackers|fallback|max_recent|max_miss|cache_control|retries)\s*=\s*(.*)$/,"usage: mogilefs (domain|trackers|fallback|max_recent|max_miss|retries) = <input>");
  my ($cmd,$result) = $mc->args;
  
  my $svcname;
  unless ($svcname ||= $mc->{ctx}{last_created}) {
      return $mc->err("No service name in context from CREATE SERVICE <name> or USE <service_name>");
  }

  my $ss = Perlbal->service($svcname);
  return $mc->err("Non-existent service '$svcname'") unless $ss;

  $ss->{extra_config}->{$cmd} = $result;
}

our %statobjs;

# called when we're being added to a service
sub register {
    my ( $class, $svc ) = @_;
    
    my $max_recent = ($svc->{extra_config}->{max_recent} eq undef) ? 100 : $svc->{extra_config}->{max_recent};
    my $max_miss = ($svc->{extra_config}->{max_miss} eq undef) ? 100 : $svc->{extra_config}->{max_miss};
    my $retries = ($svc->{extra_config}->{retries} eq undef) ? 3 : $svc->{extra_config}->{retries};
    
    my @trackers = split(/,/,$svc->{extra_config}->{trackers});
    my $mogc = MogileFS::Client->new(domain => $svc->{extra_config}->{domain},hosts  => \@trackers);  
    
    my $sobj = Perlbal::Plugin::MogileFS::Stats->new();
    $statobjs{$svc->{name}} = [ $svc, $sobj ];
    
    my $lookup_file = sub {
      my Perlbal::ClientHTTP $c = shift;
      my $hd = $c->{req_headers};
      
      #Perlbal::log('debug',url_to_key($c->{req_headers}->{uri}));
      \$sobj->{'mogilefs_requests'}++;
      

      my $mogkey = url_to_key($c->{req_headers}->{uri});
      my @paths = [];
      my $attempts = $retries;
      while($attempts > 0) {
        # MogileFS::Client can potential die here, so we eval this and retry a few times if tracker
        # failed to respond. Eval makes sure this does not kill the perlbal process.
        eval { @paths = $mogc->get_paths($mogkey); };
        last unless $@;
        $attempts-=$attempts;
      }
      
      $c->watch_read(0);
      $c->watch_write(1);

      my $miss = scalar(@paths) > 0 ? 0 : 1;
      my $code = $miss == 0 ? 200 : 404;
      my $msg = undef;
      
      \$sobj->{'mogilefs_misses'}++ if $miss;
      \$sobj->{'mogilefs_hits'}++ unless $miss;
      
      push @{$sobj->{mogilefs_recent}}, sprintf('%s  %s',  $miss == 1 ? 'MISS' : 'HIT ', $mogkey );
      shift(@{$sobj->{mogilefs_recent}}) if scalar(@{$sobj->{mogilefs_recent}}) > $max_recent;
      
      if ($miss) {
        push @{$sobj->{mogilefs_miss_recent}}, sprintf('%s  %s', 'MISS', $mogkey );
        shift(@{$sobj->{mogilefs_miss_recent}}) if scalar(@{$sobj->{mogilefs_miss_recent}}) > $max_miss;
      }
      
      # if fallback is true and mogilefs does not have anything, fallback to docroot
      return 0 if $svc->{extra_config}->{fallback} == 1 && $miss;

      my $res = $c->{res_headers} = Perlbal::HTTPHeaders->new_response($code);
      $res->header('Cache-Control',$svc->{extra_config}->{cache_control}) if defined $svc->{extra_config}->{cache_control} && $miss == 0;
      $res->header('X-Reproxy-URL',join(' ',@paths)) unless $miss;
      
      
      my $body;
      $res->header("Content-Type", "text/html");
      my $en = $res->http_code_english;
      $body = "<h1>$code" . ($en ? " - $en" : "") . "</h1>\n";
      $body .= $msg if $msg;
      $res->header('Content-Length', length($body));
      $res->header('Server', 'Perlbal');

      $c->setup_keepalive($res);

      $c->state('xfer_resp');
      $c->tcp_cork(1);  # cork writes to self
      $c->write($res->to_string_ref);
      if (defined $body) {
          unless ($c->{req_headers} && $c->{req_headers}->request_method eq 'HEAD') {
              # don't write body for head requests
              $c->write(\$body);
          }
      }
      $c->write(sub { $c->http_response_sent; });
      
      return 1;
    };
    $svc->register_hook('MogileFS', 'start_web_request', $lookup_file);

    return 1;
}

sub load {
    my $class = shift;
    
    Perlbal::register_global_hook('manage_command.mogilefs', \&mogilefs_config);
    
    Perlbal::register_global_hook('manage_command.mogilefs_stats', sub {
      my @res;

      # create temporary object for stats storage
      my $gsobj = Perlbal::Plugin::MogileFS::Stats->new();
      
      push @res, "\t";

      # dump per service
      foreach my $svc (keys %statobjs) {
          my $sobj = $statobjs{$svc}->[1];

          # for now, simply dump the numbers we have
          foreach my $key (sort @statkeys) {
              push @res, sprintf("%-15s %-25s %12d", $svc, $key, $sobj->{$key});
              $gsobj->{$key} += $sobj->{$key};
          }
      }

      push @res, "\t";

      # global stats
      foreach my $key (sort @statkeys) {
          push @res, sprintf("%-15s %-25s %12d", 'total', $key, $gsobj->{$key});
      }

      push @res, ".";
      return \@res;
    });
    
    # recent requests and if they were hits or misses
    Perlbal::register_global_hook("manage_command.mogilefs_recent", sub {
        my @res;
        foreach my $svc (keys %statobjs) {
            my $sobj = $statobjs{$svc}->[1];
            push @res, "$svc $_"
                foreach @{$sobj->{mogilefs_recent}};
        }

        push @res, ".";
        return \@res;
    });
    
    # recent request misses
    Perlbal::register_global_hook("manage_command.mogilefs_misses", sub {
        my @res;
        foreach my $svc (keys %statobjs) {
            my $sobj = $statobjs{$svc}->[1];
            push @res, "$svc $_"
                foreach @{$sobj->{mogilefs_miss_recent}};
        }

        push @res, ".";
        return \@res;
    });
    
    return 1;
}

sub unload {
    my $class = shift;
    Perlbal::unregister_global_hook('manage_command.mogilefs');
    Perlbal::unregister_global_hook('manage_command.mogilefs_stats');
    Perlbal::unregister_global_hook('manage_command.mogilefs_recent');
    Perlbal::unregister_global_hook('manage_command.mogilefs_misses');
    %statobjs = ();
    return 1;
}

# called when we're no longer active on a service
sub unregister {
    my ( $class, $svc ) = @_;
    $svc->unregister_hooks('MogileFS');
    $svc->unregister_setters('MogileFS');
    delete $statobjs{$svc->{name}};
    return 1;
}


# statistics object
package Perlbal::Plugin::MogileFS::Stats;

use fields (
    'mogilefs_requests',
    'mogilefs_misses', 
    'mogilefs_hits', 
    'mogilefs_recent',
    'mogilefs_miss_recent',
    );

sub new {
    my Perlbal::Plugin::MogileFS::Stats $self = shift;
    $self = fields::new($self) unless ref $self;

    # 0 initialize everything here
    $self->{$_} = 0 foreach @Perlbal::Plugin::MogileFS::statkeys;

    # other setup
    foreach (qw/mogilefs_recent mogilefs_miss_recent/) {
      $self->{$_} = [];
    }
    
    return $self;
}

1;

=head1 NAME

Perlbal::Plugin::MogileFS - perlbal gateway to MogileFS

=head1 SYNOPSIS

This plugin provides Perlbal the ability to serve data out of MogileFS.

URL paths are converted to ':' delimited MogileFS keys. For example, '/video/10/default.jpg' is converted to 'video:10:default.jpg'. You can change the way keys are hashed by modifying the url_to_key function. Future releases will support URL to Key conversions to be setup in the Perlbal configuration.

Configuration as follows:

  See sample/perlbal.conf
  
  -- Required Configuration Options
  
  MOGILEFS domain = <domain name>
    - Default: none
    - The default MogileFS domain to use for the service.
    
  MOGILEFS trackers = <serv1:6001,serv2:6001>
    - Default: none
    - List of trackers comma delimited.
    
  -- Optional Configuration options
    
  MOGILEFS fallback = <1 or 0>
    - Default: 0
    - Should Perlbal try the filesystem docroot if MogileFS key lookup fails.
    
  MOGILEFS max_recent = <int> 
    - Default: 100
    - Max amount of fetch records to keep for statistics. Defaults to 100.
    
  MOGILEFS max_miss = <int> 
    - Default: 100
    - Max amount of fetch records to keep for MISS statistics. Defaults to 100.
    
  MOGILEFS retries = <int> 
    - Default: 3
    - Max amount of retries on broken mogilefs tracker socket. Defaults to 3.
    
  MOGILEFS cache_control = <string>
    - Default: off
    - Suggested: max-age=2592000
    - Cache-Control headers appended to responses on HIT for forward caching proxies. 
      It is recommended to have squid or varnish in front.
  
=head1 MANAGEMENT COMMANDS

  Telnet into your Perlbal management interface that is normally configured on port 60000.

  mogilefs_stats    Fetch statistics for all mogilefs web services served by this perlbal instance.
  
  mogilelookup    mogilefs_hits                       14
  mogilelookup    mogilefs_misses                     12
  mogilelookup    mogilefs_requests                   26

  total           mogilefs_hits                       14
  total           mogilefs_misses                     12
  total           mogilefs_requests                   26
  
  mogilefs_recent   Fetch recent keys tried with MISS/HIT information. See which keys fail.
  
  mogilelookup MISS  photo:13936:ron-and-jan.tif:640
  mogilelookup MISS  favicon.ico
  mogilelookup MISS  photo:13936:ron-and-jan.tif:640
  mogilelookup HIT   video:100:default.jpg
  mogilelookup HIT   video:799:default.jpg:130
  
  mogilefs_misses   Fetch recent keys that came back as a MISS. See which keys fail.
  
  mogilelookup MISS  photo:13936:ron-and-jan.tif:640
  mogilelookup MISS  favicon.ico
  mogilelookup MISS  photo:13936:ron-and-jan.tif:640

=head1 AUTHOR

  Victor Igumnov, C<< <victori at fabulously40.com> >>

=cut
