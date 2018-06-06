delete from mupit.Genome2PDB where PDBId ='fENSP00000380152_7_B';
load data local infile 'C:/Users/Kyle/a/mupit-brca/Genome2PDB.txt' into table mupit.Genome2PDB;
select * from mupit.Genome2PDB where PDBId ='fENSP00000380152_7_B';
delete from mupit.Genome2PDB where PDBId ='fENSP00000380152_7_B';
load data local infile 'C:/Users/Kyle/a/mupit-brca/Sprot2PDB.txt' into table mupit.Sprot2PDB;
select * from mupit.Sprot2PDB where pdbId ='fENSP00000380152_7_B';

delete from mupit.structure_summary where structure_id='fENSP00000380152_7';
insert into mupit.structure_summary values ('fENSP00000380152_7','BRCA2','Homology model of BRCA2','theoretical',760,0,0,0,1);
select * from mupit.structure_summary where structure_id='fENSP00000380152_7';

delete from mupit.PDB_Info where pdbId like 'fENSP00000380152_7_%';
insert into mupit.PDB_Info values ('fENSP00000380152_7_A','BRCA2','Homology model of BRCA2','ENSP',null,1);
insert into mupit.PDB_Info values ('fENSP00000380152_7_B','BRCA2','Homology model of BRCA2','ENSP',null,1);
insert into mupit.PDB_Info values ('fENSP00000380152_7_C','BRCA2','Homology model of BRCA2','ENSP',null,1);
select * from mupit.PDB_Info where pdbId like 'fENSP00000380152_7_%';

delete from mupit.biomolecules where pdbId='fENS';
insert into mupit.biomolecules values ('fENS','A,B,C',1,'A,B,C');
select * from mupit.biomolecules where pdbId='fENS';

delete from mupit.chaindesc where pdbId='fENS';
insert into mupit.chaindesc values ('fENS','Deleted In Split Hand/split Foot Protein 1','A');
insert into mupit.chaindesc values ('fENS','Breast Cancer Type 2 Susceptibility Protein','B');
insert into mupit.chaindesc values ('fENS','SSDNA Fragment','C');
select * from mupit.chaindesc where pdbId = 'fENS';
