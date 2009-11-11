package Perlbal::Plugin::AsyncMogileFS;

use Perlbal;
use strict;
use warnings;
use Data::Dumper;
use MogileFS::Client;

#
# LOAD AsyncMogileFS
# SET plugins = AsyncMogileFS

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
  my $mc = shift->parse(qr/^mogilefs\s*(domain|trackers|max_recent|max_miss|fallback|retries|cache_control|noverify)\s*=\s*(.*)$/,"usage: mogilefs (domain|trackers|max_recent|max_miss|fallback|retries|noverify) = <input>");
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
    
    # sane defaults
    $svc->{extra_config}->{noverify} = 1 if $svc->{extra_config}->{noverify} eq undef;
    $svc->{extra_config}->{fallback} = 0 if $svc->{extra_config}->{fallback} eq undef;
    $svc->{extra_config}->{max_recent} = 100 if $svc->{extra_config}->{max_recent} eq undef;
    $svc->{extra_config}->{max_miss} = 100 if $svc->{extra_config}->{max_miss} eq undef;
    $svc->{extra_config}->{retries} = 3 if $svc->{extra_config}->{retries} eq undef;
    
    my @trackers = split(/,/,$svc->{extra_config}->{trackers});
    
    my $sobj = Perlbal::Plugin::AsyncMogileFS::Stats->new();
    $statobjs{$svc->{name}} = [ $svc, $sobj ];
    
    my $lookup_file = sub {
      my Perlbal::ClientHTTP $c = shift;
      my $hd = $c->{req_headers};
      
      \$sobj->{'mogilefs_requests'}++;
      
      my $mogkey = url_to_key($c->{req_headers}->{uri});
      Perlbal::Plugin::AsyncMogileFS::AsyncRequest->new(
        domain   => $svc->{extra_config}->{domain},
        noverify => $svc->{extra_config}->{noverify},
        timeout  => 5,
        retries  => $svc->{extra_config}->{retries},
        key      => $mogkey,
        trackers => \@trackers,
        callback => sub {
          my $response = shift;

          my @paths = ();
          if (defined $response && $response =~ /OK /) {
            eval {
              my $res = substr($response,3);
              $res = _decode_url_string($res);
              @paths = map { $res->{"path$_"} } (1..$res->{paths});
            }
          }

          $c->watch_read(0);
          $c->watch_write(1);
          
          my $miss = scalar(@paths) > 0 ? 0 : 1;
          if ($miss) {
            \$sobj->{'mogilefs_misses'}++;
            push @{$sobj->{mogilefs_miss_recent}}, sprintf('%s  %s', 'MISS', $mogkey );
            shift(@{$sobj->{mogilefs_miss_recent}}) if scalar(@{$sobj->{mogilefs_miss_recent}}) > $svc->{extra_config}->{max_miss};
          }
          \$sobj->{'mogilefs_hits'}++;
          push @{$sobj->{mogilefs_recent}}, sprintf('%s  %s',  $miss == 1 ? 'MISS' : 'HIT ', $mogkey );
          shift(@{$sobj->{mogilefs_recent}}) if scalar(@{$sobj->{mogilefs_recent}}) > $svc->{extra_config}->{max_recent};
          
          if ($miss) {
            if($svc->{extra_config}->{fallback} == 1) {
              # GET/HEAD requests (local, from disk)
              if ($hd->request_method eq 'GET' || $hd->request_method eq 'HEAD') {
                  # and once we have it, start serving
                  $c->watch_read(0);
                  return $c->_serve_request($hd);
              }
            }
            # else, bad request
            return $c->send_response(404);
          }

          my $res = $c->{res_headers} = Perlbal::HTTPHeaders->new_response(200);
          $res->header('Cache-Control',$svc->{extra_config}->{cache_control}) if defined $svc->{extra_config}->{cache_control};
          $res->header('X-Reproxy-URL',join(' ',@paths));
          $res->header('Server', 'Perlbal');

          $c->setup_keepalive($res);

          $c->state('xfer_resp');
          $c->tcp_cork(1);  # cork writes to self
          $c->write($res->to_string_ref);
          $c->write(sub { $c->http_response_sent; });

          return 1;
        }
      );
      
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
      my $gsobj = Perlbal::Plugin::AsyncMogileFS::Stats->new();
      
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

sub _decode_url_string {
    my $arg = shift;
    my $buffer = ref $arg ? $arg : \$arg;
    my $hashref = {};  # output hash

    my $pair;
    my @pairs = split(/&/, $$buffer);
    my ($name, $value);
    foreach $pair (@pairs) {
        ($name, $value) = split(/=/, $pair);
        $value =~ tr/+/ /;
        $value =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
        $name =~ tr/+/ /;
        $name =~ s/%([a-fA-F0-9][a-fA-F0-9])/pack("C", hex($1))/eg;
        $hashref->{$name} .= $hashref->{$name} ? "\0$value" : $value;
    }

    return $hashref;
}

# statistics object
package Perlbal::Plugin::AsyncMogileFS::Stats;

use fields (
    'mogilefs_requests',
    'mogilefs_misses', 
    'mogilefs_hits', 
    'mogilefs_recent',
    'mogilefs_miss_recent',
    );

sub new {
    my Perlbal::Plugin::AsyncMogileFS::Stats $self = shift;
    $self = fields::new($self) unless ref $self;

    # 0 initialize everything here
    $self->{$_} = 0 foreach @Perlbal::Plugin::AsyncMogileFS::statkeys;

    # other setup
    foreach (qw/mogilefs_recent mogilefs_miss_recent/) {
      $self->{$_} = [];
    }
    
    return $self;
}

package Perlbal::Plugin::AsyncMogileFS::AsyncRequest;
 
use base 'Danga::Socket';
use fields qw(callback);
use IO::Socket;
use Data::Dumper;
use Socket;
use IO::Handle;


sub new {
    my Perlbal::Plugin::AsyncMogileFS::AsyncRequest $self = shift;
    my %args = @_;
    
    $self = fields::new($self) unless ref $self;
 
    $self->{callback} = $args{callback};

    my $sock;
    
    foreach (1..$args{retries}) {
      foreach (@{$args{trackers}}) {
        eval { 
          $sock = IO::Socket::INET->new($_); 
          IO::Handle::blocking($sock, 0);
        };
        last if defined $sock;
      }
      last if defined $sock;
    }
    
    unless (defined $sock) {
      $self->close;
      $self->{callback}->(undef) unless defined $sock;
      return $self;
    }
    
    $self->SUPER::new( $sock );
    my $client = $self;
    $self->AddTimer( $args{timeout}, sub {
      return unless defined $self->{sock};
      $self->watch_read(0);
      $self->close;
      $self->{sock} = undef;
      $self->{callback}->(undef);
    } );
    $self->watch_read(1);
    
    $self->write( "get_paths domain=$args{domain}&noverify=$args{noverify}&key=$args{key}\r\n");
    
    return $self;
}


sub event_read {
    my $self = shift;
    
    my $sock = $self->{sock};
    my $response = "";
    
    while($response !~ "\r\n") {
      my $data;
      $sock->read($data,1);
      $response.=$data;
    }
    #path1=http://xxx.xxx.xxx.xxx:7500/dev1/0/000/000/0000000001.fid&paths=1
    
    $self->watch_read(0);
    $self->close;
    $self->{sock} = undef;
    $self->{callback}->($response);
}

1;

=head1 NAME

Perlbal::Plugin::AsyncMogileFS - Asynchronous Perlbal MogileFS Implementation

=head1 SYNOPSIS

This plugin provides Perlbal the ability to serve data out of MogileFS asynchronously.

URL paths are converted to ':' delimited MogileFS keys. For example, '/video/10/default.jpg' is converted to 'video:10:default.jpg'. You can change the way keys are hashed by modifying the url_to_key function. Future releases will support URL to Key conversions to be setup in the Perlbal configuration.

See Perlbal::Plugin::MogileFS for configuration documentation.

=head1 AUTHOR

  Victor Igumnov, C<< <victori at fabulously40.com> >>

=cut
