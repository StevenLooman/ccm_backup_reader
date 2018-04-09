*Database structure

From IBM article [Recovering a Corrupt Table Within a CM Synergy Database (Windows NT
Server)](https://www-01.ibm.com/support/docview.wss?uid=swg21325228):

```
create table attrib
(
id                serial(10001),
name             varchar(32,18),
modify_time      integer,
textval          text,
binval           byte,
strval           varchar(64,64),
intval           integer,
floatval         float,
is_attr_of       integer,
has_attype       integer
) extent size 4096 next size 16384 lock mode row; 
```

```
create table compver                   
(                                   
id               serial(10001),      
status           varchar(16,6),      
create_time      integer,            
modify_time      integer,            
owner            varchar(32,12),     
is_asm           integer,            
is_model         integer,            
subsystem        varchar(48,12),     
cvtype           varchar(16,12),     
name             varchar(155,18),    
version          varchar(32,12),     
local_bgraph     integer,            
ui_info          text,               
has_cvtype       integer,            
has_model        integer,            
has_super_type   integer             
) extent size 512 next size 1024 lock mode row; 
```

[How to recognize and correct ccmdb check errors in Rational Synergy with Informix On-Line 7.x, 9.x, 10.x and 11.x database server](https://www-01.ibm.com/support/docview.wss?uid=swg21325223)

```
delete from bind where has_asm=11033 and has_bound_bs=33268;
delete from bind where has_asm=11033 and has_bound_bs=33269;

delete from attrib where is_attr_of = 15252; 
delete from attrib where is_attr_of = 15343; 

update bsite set has_next_bs = 11626 where id = 11625;
update bsite set has_next_bs = 11640 where id = 11639; 

delete from bind where has_parent=25418 and has_asm=55819; 

has_asm      = Non-existent (cvid = 123456)
has_bound_bs = 1/project/my_project (bsid = 54321)
has_child    = 1/project/my_project/1 (cvid = 54322)
has_parent   = 1/dir/my_project/1 (cvid = 54323)
select * from bind where has_asm=123456;
delete from bind where has_asm=123456;

delete from relate where from_cv=139739;
delete from relate where to_cv=58781;

```


[Text encoding and the Illegal Character Detection
tool](https://www.ibm.com/support/knowledgecenter/en/SSRNYG_7.2.1/com.ibm.rational.synergy.upgrade.win.doc/topics/s_t_upw_text_encoding.html)
```
When a Rational Synergy database is upgraded from 7.0 or 7.1 to 7.2 or later, all text metadata (type and object names, string and text
attribute values, and similar items) in that database is converted from the Windows CP1252 encoding used in previous releases to the UTF-8
encoding used in Release 7.2 or later.
```


[](https://www-01.ibm.com/support/docview.wss?uid=swg21325223)
