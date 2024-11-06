###### Creating operational layer

create schema if not exists imdb_full;
use imdb_full;

### Dropping unnecessary tables

drop table movies2producers;
drop table movies2editors;
drop table movies2writers;
drop table business;
drop table countries;
drop table distributors;
drop table editors;
drop table writers;
drop table prodcompanies;
drop table producers;

### Adding a timestamp variable to ratings table that is supposed to be updated

alter table ratings
add datestamp date;

set sql_safe_updates = 0;
update ratings
set datestamp = current_date();
set sql_safe_updates = 1;

###### Creating analytical layer

### Calculating appproximate variance of the ratings of each film
 
drop procedure if exists calculate_variance;
delimiter //

create procedure calculate_variance()
begin
	declare i int default 1;
    declare count int;
    declare digit int;
    declare current_id int;
    declare dist_string varchar(10);
    declare done boolean default false;
	declare score_cursor cursor for select movieid, distribution from ratings where not distribution = '';
    declare continue handler for not found set done = true;
    
	create table if not exists expanded_values (
		score_id int,
        value int
	);
	truncate table expanded_values;
    drop table if exists ratings_var;
    create table ratings_var
		(movieid int,
        approx_var double)
        ;
 
    open score_cursor;
    
    read_loop: loop
		fetch score_cursor into current_id, dist_string;
        
        if done then
			leave read_loop;
		end if;
        
        truncate table expanded_values;
        
        set i=1;
        while i<11 do
			if substring(dist_string, i, 1) = '.' then
				set digit = 0;
			else
				set digit = cast(substring(dist_string, i, 1) as unsigned);
			end if;
            set count = 1;
            while count < digit+1 do
				insert into expanded_values(score_id, value)
                values (current_id, i);
                set count = count + 1;
			end while;
            set i = i+1;
		end while;
        
        set @var = (select var_pop(value) from expanded_values);
        
        insert into ratings_var (movieid, approx_var)
        values (current_id, @var);
        
	end loop;
    close score_cursor;
    
end//

delimiter ;
call calculate_variance();



### Creating denormalised table for directors and scores

drop table if exists dir_denorm;
create table dir_denorm as
	select
		movies2directors.movieid as movieid,
        movies2directors.directorid as directorid,
        directors.name as director,
		movies.title as title,
        movies.year as year,
        movies2directors.genre as genre,
        ratings.rank as score,
        ratings.votes as votes,
		ratings.distribution as dist
	from
		movies2directors
    inner join
		movies using (movieid)
	inner join
		ratings using (movieid)
	inner join
		directors using (directorid);
        
select count(*) from (select count(*) as a from dir_denorm group by movieid, directorid having not a=1) as rr;
select count(*) from (select count(*) as a from dir_denorm group by movieid, directorid having a=1) as rr;


### Creating a working table for average scores of all directors with the numbers of films

drop procedure if exists create_dir_rat;
delimiter //

create procedure create_dir_rat()
begin

drop table if exists dir_rat;
create table dir_rat as
	select
		directorid as directorid,
        director as director,
        avg(score) as avg_score,
        count(score) as num_films,
        sum(votes) as votes
	from dir_denorm
    group by directorid, director, dirnum;
    
end//

delimiter ;
call create_dir_rat();


### Creating denormalised table for actors and scores

drop table if exists act_denorm;
create table act_denorm as
	select
		movies2actors.movieid as movieid,
        movies2actors.actorid as actorid,
        actors.name as actor,
		movies.title as title,
		movies2actors.leading as role_lead,
        actors.sex as sex,
        movies.year as year,
        ratings.rank as score,
        ratings.votes as votes,
		ratings.distribution as dist
	from
		movies2actors
    inner join
		movies using (movieid)
	inner join
		ratings using (movieid)
	inner join
		actors using (actorid);
        
select count(*) from (select count(*) as a from act_denorm group by movieid, actorid having not a=1) as rr;
select count(*) from (select count(*) as a from act_denorm group by movieid, actorid having a=1) as rr;

        

### Creating a working table for average scores of all leading artists with the numbers of films (only those playing the leading role are considered)

drop procedure if exists create_act_rat;
delimiter //

create procedure create_act_rat()
begin

drop table if exists act_rat;
create table act_rat as
	select
		actorid as actorid,
        actor as actor,
        avg(score) as avg_score,
        count(score) as num_films,
        sum(votes) as votes
	from act_denorm
    where role_lead=1
    group by actorid, actor;
end//

delimiter ;
call create_act_rat();


### Creating an aggregated table for movies and the calculated data
### (group by movie since there are some movies which were directed
### by more than one person, so for them the average director's score
### is the average of the average scores of all its directors

drop procedure if exists create_aggreg;
delimiter //

create procedure create_aggreg()
begin
drop table if exists aggreg;
create table aggreg as
	select
		movies.movieid as movieid,
        movies.title as title,
        movies.year as year,
        ratings.rank as film_score,
        movies2actors_lead.actorid as actorid,
        actors.sex as actor_sex,
        act_rat.avg_score as act_score,
#        movies2directors.directorid as directorid,
        avg(dir_rat.avg_score) as dir_score,
        act_rat.num_films as act_films,
        dir_rat.num_films as dir_films,
        ratings_var.approx_var as approx_var
	from
		movies
	inner join
		ratings using (movieid)
	inner join
		(select * from movies2actors where `leading`=1) as movies2actors_lead
			using (movieid)
	inner join
		actors on actors.actorid = movies2actors_lead.actorid
	inner join
		act_rat 
            on act_rat.actorid = movies2actors_lead.actorid
	inner join
		movies2directors using (movieid)
	inner join
		dir_rat
			on dir_rat.directorid = movies2directors.directorid
	inner join
		ratings_var using (movieid)
	group by movieid, title, year, film_score, actorid, act_score, act_films, dir_films, approx_var, sex;
end//

delimiter ;
call create_aggreg();

select * from dir_denorm;





drop procedure if exists update_variance;
delimiter //

create procedure update_variance(
			in dist_string varchar(10),
            out approx_var double)
begin
	declare i int default 1;
    declare count int;
    declare digit int;
    declare dist_string varchar(10);
    
	create table if not exists expanded_values (
		score_id int,
        value int
	);
	truncate table expanded_values;
        
        set i=1;
        while i<11 do
			if substring(dist_string, i, 1) = '.' then
				set digit = 0;
			else
				set digit = cast(substring(dist_string, i, 1) as unsigned);
			end if;
            set count = 1;
            while count < digit+1 do
				insert into expanded_values(score_id, value)
                values (current_id, i);
                set count = count + 1;
			end while;
            set i = i+1;
		end while;
        
        select var_pop(value)
        into approx_var
        from expanded_values;
        
end//

drop trigger if exists update_all;

create trigger update_all
after update
on ratings for each row
begin
	update dir_denorm
		set
			score = new.rank,
            votes = new.votes
		where dir_denorm.movieid = new.movieid;
    update act_denorm
		set
			score = new.rank,
            votes = new.votes
		where act_denorm.movieid = new.movieid;
	call update_variance(new.distribution, @approx_var);
    update ratings_var
		set approx_var = @approx_var
        where ratings_var.movieid = new.movieid
			
	call create_dir_rat();
    call create_act_rat();
    call create_aggreg();

end //

delimiter ;


set sql_safe_updates = 0;
update ratings
	set
		`rank` = 8.9,
        votes = 10000000,
        distribution = '0100000008'
	where movieid = 1672052;
set sql_safe_updates = 1;
		

;
select * from ratings;
select * from aggreg;
	



select * from aggreg;
select * from ratings;
    

select * from movies2actors where leading=1;


select * from movies2actors;
select * from movies;
select count(*) from movies;
select * from runningtimes;
select * from ratings;
select * from movies where movieid=1674737;
select * from movies join ratings on movies.movieid=ratings.movieid;
select year from movies group by year;
select count(movieid) as lala from movies2directors group by directorid order by lala desc;
select count(movieid) as lala from movies2directors;
select count(movieid) as lala from movies2directors group by movieid order by lala desc;
select count(*) from movies2actors;
select count(*) from movies2actors where movies2actors.leading=1;
select actors.name, movies.title
from movies2actors
join movies on movies.movieid=movies2actors.movieid
join actors on actors.actorid=movies2actors.actorid
where movies2actors.leading=1;
select * from ratings





