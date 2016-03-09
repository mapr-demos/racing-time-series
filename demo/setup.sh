apt-get update
apt-get install -y ca-certificates build-essential libxmu-dev libxmu6 libxi-dev libxine-dev libalut-dev freeglut3 freeglut3-dev cmake libogg-dev libvorbis-dev libxxf86dga-dev libxxf86vm-dev libxrender-dev libxrandr-dev zlib1g-dev libpng12-dev libplib-dev wmctrl

cd /vagrant
wget -nc http://sourceforge.net/projects/torcs/files/all-in-one/1.3.6/torcs-1.3.6.tar.bz2/download
cd -
cp /vagrant/download ./torcs-1.3.6.tar.bz2
tar xfvj torcs-1.3.6.tar.bz2

cd torcs-1.3.6
patch -p1 < /vagrant/src.diff

./configure --enable-debug
make
make install
make datainstall


echo 'deb http://mapr-partner.s3.amazonaws.com/ecosystem-5.x/ubuntu binary/' >> /etc/apt/sources.list
echo 'deb http://mapr-partner.s3.amazonaws.com/v5.1.0/ubuntu mapr optional' >> /etc/apt/sources.list

apt-get update --allow-unauthenticated 
apt-get install mapr-kafka -y --allow-unauthenticated
add-apt-repository ppa:webupd8team/java -y
apt-get update -y
echo debconf shared/accepted-oracle-license-v1-1 select true | debconf-set-selections
echo debconf shared/accepted-oracle-license-v1-1 seen true | debconf-set-selections
apt-get install oracle-java8-installer -y
echo '192.168.42.2 centos7-sn' >> /etc/hosts
/opt/mapr/server/configure.sh -N cyber.mapr.cluster -c -C centos7-sn:7222 -HS centos7-sn -Z centos7-sn
useradd mapr -u 5000

apt-get install -y maven
cd /vagrant/TelemetryAgent
MAVEN_OPTS=-Xss256m mvn clean install
cd /vagrant/rest-mock
MAVEN_OPTS=-Xss256m mvn clean install

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
