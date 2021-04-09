#!/bin/bash

PACKAGENAME=$1
echo Building RPMs..
GITROOT=`git rev-parse --show-toplevel`
cd $GITROOT
if [ ! -n "$3" ] ;then
   VER=1.3.9
else
   VER=${3//-/_}
fi
echo "Git get full depth..."
git fetch --unshallow
if [ ! -n "$4" ] ;then
   REL=`git rev-parse --short HEAD`git
   REL=`git log --oneline|wc -l`.$REL
   REL=$REL
else
   REL=`git rev-parse --short HEAD`git
   REL=`git log --oneline|wc -l`.$REL
   REL=$4_$REL
fi

BUILDROOT=$2
RPMTOPDIR=$GITROOT/$BUILDROOT
echo "Ver: $VER, Release: $REL"


rm -rf $RPMTOPDIR
# Create tarball
mkdir -p $RPMTOPDIR/{SOURCES,SPECS}
git archive --format=tar --prefix=${PACKAGENAME}/ HEAD | gzip -c > $RPMTOPDIR/SOURCES/${PACKAGENAME}-${VER}-${REL}.tar.gz

# Convert git log to RPM's ChangeLog format (shown with rpm -qp --changelog <rpm file>)
sed -e "s/%{ver}/$VER/" -e "s/%{rel}/$REL/" $GITROOT/package/${PACKAGENAME}.spec > $RPMTOPDIR/SPECS/${PACKAGENAME}.spec
git log -n 10 --format="* %cd %aN%n- (%h) %s%d%n" --date=local | sed -r 's/[0-9]+:[0-9]+:[0-9]+ //' >> $RPMTOPDIR/SPECS/${PACKAGENAME}.spec
# Build SRC and binary RPMs
rpmbuild \
--define "_topdir $RPMTOPDIR" \
--define "_rpmdir $PWD" \
--define "_srcrpmdir $PWD" \
--define '_rpmfilename %%{NAME}-%%{VERSION}-%%{RELEASE}.%%{ARCH}.rpm' \
-ba $RPMTOPDIR/SPECS/${PACKAGENAME}.spec

