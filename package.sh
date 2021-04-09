BASEDIR=$(pwd)
WORKDIR="/work"
BUILDROOT="rpm-build"
BUILDDIR=$(WORKDIR)/$(BUILDROOT)/BUILD/br
echo $BASEDIR
echo $BUILDDIR
sudo docker run --rm -v ${BASEDIR}:${BUILDDIR} -w ${BUILDDIR} journeymidnight/yig bash -c 'make package'