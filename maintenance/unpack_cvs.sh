rm -rf /usr/local/lib/CVS/*
cp /ext/temp/CVS.tar.gz /usr/local/lib/CVS
tar -xzf /usr/local/lib/CVS/CVS.tar.gz -C /usr/local/lib/CVS
chmod +x /usr/local/lib/CVS/CHASM/*
chmod +x /usr/local/lib/CVS/SNVBox/*
chmod +x /usr/local/lib/CVS/VEST/*
chmod +x /usr/local/lib/CVS/PARF/*
rm /usr/local/lib/CVS/CVS.tar.gz