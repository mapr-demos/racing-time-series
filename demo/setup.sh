MAPR_UID=${MAPR_UID:-5000}
CLUSTER_IP=${CLUSTER_IP:-}
CLUSTER_HOST=${CLUSTER_HOST:-mapr-cluster}
CLUSTER_NAME=${CLUSTER_NAME:-mapr.cluster}

source /vagrant/config.conf

# Install dependencies

apt-get update
apt-get install -y ca-certificates build-essential libxmu-dev libxmu6 libxi-dev libxine-dev libalut-dev freeglut3 freeglut3-dev cmake libogg-dev libvorbis-dev libxxf86dga-dev libxxf86vm-dev libxrender-dev libxrandr-dev zlib1g-dev libpng12-dev libplib-dev wmctrl

# Download sources of TORCS

cd /vagrant
wget -nc http://sourceforge.net/projects/torcs/files/all-in-one/1.3.6/torcs-1.3.6.tar.bz2/download
cd -
cp /vagrant/download ./torcs-1.3.6.tar.bz2
tar xfvj torcs-1.3.6.tar.bz2

# Apply the patch to store telemetry
cd torcs-1.3.6
patch -p1 < /vagrant/src.diff

# Compile the TORCS binary
./configure --enable-debug
make
make install
make datainstall

# Setup MapR Client
echo 'deb http://mapr-partner.s3.amazonaws.com/ecosystem-5.x/ubuntu binary/' >> /etc/apt/sources.list
echo 'deb http://mapr-partner.s3.amazonaws.com/v5.1.0/ubuntu mapr optional' >> /etc/apt/sources.list

apt-get update --allow-unauthenticated 
apt-get install mapr-kafka -y --allow-unauthenticated
add-apt-repository ppa:webupd8team/java -y
apt-get update -y
echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
apt-get install oracle-java8-installer -y

if [ -n "${CLUSTER_IP}" ]
	then echo "${CLUSTER_IP} ${CLUSTER_HOST}" >> /etc/hosts
fi
/opt/mapr/server/configure.sh -N "${CLUSTER_NAME}" -c -C "${CLUSTER_HOST}":7222 -HS "${CLUSTER_HOST}" -Z "${CLUSTER_HOST}"

useradd mapr -u ${MAPR_UID}

# Build the TelemetryAgent (MapR Streams producers/consumers) and UI Server

apt-get install -y maven
cd /vagrant/TelemetryAgent
MAVEN_OPTS=-Xss256m mvn clean install
cd /vagrant/rest-mock
MAVEN_OPTS=-Xss256m mvn clean install

# Add the launcher to the desktop

cat > /home/vagrant/Desktop/Streams-Demo.desktop << EOF
#!/usr/bin/env xdg-open

[Desktop Entry]
Version=1.0
Type=Application
Terminal=true
Exec=/vagrant/run.sh
Name=Streams Demo
Comment=executes the streams demo
Icon=/usr/share/icons/gnome/48x48/status/starred.png
EOF

chmod +x /home/vagrant/Desktop/Streams-Demo.desktop
