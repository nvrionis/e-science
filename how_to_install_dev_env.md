#Create developer's environment

##Table of Contents
1. General Information
2. Installation Procedures
3. Credentials
4. Initial configuration of new Debian image

##General Information
* Virtual Box: 4.3.12-93773 (with Oracle_VM_VirtualBox_Extension_Pack-4.3.12-93733)
* OS:  Debian 7.5.0 amd64 (64bits)
* Ram: 1.0GB
* Disk: 8GB

##Installation procedures
* Start VirtualBox 
* Select Machine | New from the menu 
  1.	Name and operating system: Name: Debian-x64 | Type: Linux | Version: Debian (64 bit), click Next
  2.	Memory size: 1.0GB, click next
  3.	Hard drive
      * Choose Create a virtual hard drive now, click Create
      * Choose VDI (VirtualBox Disk Image) in Hard drive file type, click Next
      * Dynamically allocated in Storage on physical hard drive, click Next
      * File location and size: Select a path for the virtual disk image & 8 GB for size
      * Click Create
  4. Select Debian-x64 | Settings 
      * Select Display | Video Memory: 128 MB & Disable 3D Acceleration
      * Select Storage | Controller SATA | Debian-x64.vdi | Attributes | Hard Disk: Setup the virtual hard disk (icon) |  Choose a virtual hard disk file | Find and select the VDI image, click Open, click OK
* Start the Virtual Machine

> > NOTE: If the Debian (64 bit) choice is not available in Version setting of Name and operating system Tab, make sure that Intel Virtualization Technology (IVT) is enabled in the host UEFI / BIOS.

##Credentials
* User: developer

##Initial configuration of new Debian image
###Enabling root GUI login
Comment out the following line in gdm3 file

      su root
      vi /etc/pam.d/gdm3
      "#auth required pam_succeed_if.so user != root quiet_success"

###Improve vi editor

      apt-get install vim

###Installation of curl and g++

      apt-get install curl
      apt-get install g++
    
###Change the keyboard layout
* Applications->System Tools->Preferences->System Settings
* In Region and Language ->Layouts tab and add Greek and English (US) and remove English(UK).

###Install Kamaki 
Instructions based on Kamaki 0.12.9 documentation | https://www.synnefo.org/docs/kamaki/latest/installation.html

      deb http://apt.dev.grnet.gr wheezy/
      sudo curl https://dev.grnet.gr/files/apt-grnetdev.pub|apt-key add -
      sudo apt-get update
      sudo apt-get install kamaki
      
###Install Java 1.7.0_60 oracle
1. Add a “contrib” component to /etc/apt/sources.list , e.g. deb http://.debian.net/debian/ wheezy main contrib
2. Update the list of available packages and install the java.package: apt-get update && apt-get install java-package && exit
3. Download the desired java JDK/JRE binary distribution (here is Java1.7.0_60 oracle).
4. Use java-package to create a debian  package, e.g. make-jpkg "my_downloaded_java.tar.gz"  
5. Install the binary package created: e.g. dpkg –i oracle-j2sdk1.7_1.7.0=update45_amd64.deb
6. Override the default and use a specific java version compatible with Eclipse, e.g. update-alternatives –config java s

###Install Eclipse Luna 4.4 (with python plugin)
Download Eclipse Luna 4.4, extract it and add to eclipse.ini the following lines 

    openFile
    --lancher.GTK_version
    2
    -vm
    /usr/lib/jvm/j2sdk1.7-oracle/jre/bin/java

###Install Ansible
Installation of Ansible (server)

      sudo apt-get install pip
      sudo pip install paramiko PyYAML jinja2 httplib2
      sudo apt-get install openssh-server
      sudo apt-get install ansible


Installation of Ansible (clients)

      install apt-get python python-apt
      sudo apt-get install ansible