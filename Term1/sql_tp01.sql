###### OPERATIONAL LAYER

create schema if not exists imdb_full;
use imdb_full;

### Dropping unnecessary tables

drop table if exists movies2producers;
drop table if exists movies2editors;
drop table if exists movies2writers;
drop table if exists business;
drop table if exists countries;
drop table if exists distributors;
drop table if exists editors;
drop table if exists writers;
drop table if exists prodcompanies;
drop table if exists producers;



###### ANALYTICS

# For the project, I decided to work with the iMDB dataset that was stored within Workbench 
# My first idea was to research who plays a more defining role in the public rating of the film:
# the director or the artist? For that, I decided to create a table with movies and its ratings,
# as well as with directors' and actors' average rating across all their films, so that it would 
# be later (not here) possible to run a linear regression and see which coefficient is higher.
# For that aim, I decided to take only the leading actor (since they are most defining character
# for the public score, as well as all the directors. Since some films have several directors,
# I had to average the results of director's average rating by film (across all film's directors.
# Furthermore, I wanted to study variance in ratings. For that I devised a complex procedure that
# translate a string vaguely describing the distribution of scores into approximate variance.
# Later with this data I was able to see which genres are most controversial.
# Also I could check whether films with females or males in the leading role are better marked.
# And finally I was able to create a watchlist of the best films of the 70s.



###### ANALYTICAL LAYER

### Calculating appproximate variance of the ratings of each film
# the distribution is expressed in a 10-digit string form. Each digit corresponds to
# a specific rate (the first digit to mark 1, second — 2, tenth - 10, and so on).
# Each digit represents floor(n_mark/n_all*10). If there is not one such mark, it is '.'
# So, '2.00000012' would represent that there are
# no marks 2
# 10-20% marks 9
# 20-30% marks 1 and 10
# 0-10% all the rest of marks
# To approximate variance I create an auxiliary table for each distribution string
# where I store the corresponding approximation of marks
# (for our example above it would be 1,1,9,10,10)
# and calculate variance of this set to store it in a new table ratings_var
# !!! If the event is activated, the procedure may need to be called several times.

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
        approx_var double);
 
    open score_cursor;
    
    read_loop: loop
		fetch score_cursor into current_id, dist_string;
        
        if done then
			leave read_loop;
		end if;
        
        truncate table expanded_values;
        
        # Get the number of each mark in digit
        set i=1;
        while i<11 do
			if substring(dist_string, i, 1) = '.' then
				set digit = 0;
			else
				set digit = cast(substring(dist_string, i, 1) as unsigned);
			end if;
            set count = 1;
            
            # Add the mark digit times to the set
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


### Directors and Actors averages

# For my central aim, I decided to create several working tables to calculate
# the averages for directors and artists.
# First, I creating denormalised table for directors and join the scores

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
        
# Then, I create a table for average scores of each director also with the numbers of films

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
    group by directorid, director;
    
end//

delimiter ;
call create_dir_rat();

# Then I do the same for actors

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
        
# However, here I only consider the leading actor, as they are ranked
# and as the leading actor plays the most important role for film's score.

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
### is the average of the average scores of all its directors)

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
        (select genre from movies2directors where movies2directors.movieid = movies.movieid limit 1) as genre,
        movies2actors_lead.actorid as actorid,
        actors.sex as actor_sex,
        act_rat.avg_score as act_score,
        avg(dir_rat.avg_score) as dir_score,
        act_rat.num_films as act_films,
        dir_rat.num_films as dir_films,
        ratings.votes as film_votes,
        ratings.distribution as score_dist,
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
	group by movieid, title, year, film_score, actorid, act_score, act_films,
			dir_films, approx_var, actor_sex, film_votes, score_dist, genre;
end//

delimiter ;
call create_aggreg();

select * from aggreg;



###### ETL PIPELINE

### Trigger

# For the updates of the database, I decided to consider the most obvious and
# continuous update — the update of user ratings. After the ratings are changed —
# I consider the table being updated with new information for movieid —
# the corresponding changes are made to the analytic tables.

# Because of the complicated structure of data and many auxilliary tables,
# it was nearly impossible to update all of them by trigger, as it does not
# allow for using procedures that alter data structures.
# I decided to compomise it in this way:
# I will triger only the first denorm tables for actors and directors that 
# may be easily amended selectively:

drop trigger if exists update_primary_denorms;

delimiter //

create trigger update_primary_denorms
after update
on ratings for each row
begin
	update dir_denorm
		set
			score = new.rank,
            votes = new.votes,
            dist = new.distribution
		where dir_denorm.movieid = new.movieid;
    update act_denorm
		set
			score = new.rank,
            votes = new.votes,
			dist = new.distribution
		where act_denorm.movieid = new.movieid;

end //

delimiter ;


### Event

# To introduce all the other changes that require altering data structures
# and using stored procedures, I decided to use an event that recombines
# all the tables with the new data every minute.
# The result of the change in aggreg can be tested in the end of the script.

drop event if exists update_all;
delimiter // 

create event update_all
on schedule every 1 minute
do
begin
	call calculate_variance();
	call create_dir_rat();
    call create_act_rat();
    call create_aggreg();
end //
delimiter ;


### Testing triggers

set sql_safe_updates = 0;

	select * from dir_denorm where movieid = 1672052;
	update ratings
		set
			`rank` = 8.9,
			votes = 10000000,
			distribution = '0100000008'
		where movieid = 1672052;
	select * from dir_denorm where movieid = 1672052;
	update ratings
		set
			`rank` = 7.8,
			votes = 8111,
			distribution = '0000001222'
		where movieid = 1672052;
	select * from dir_denorm where movieid = 1672052;
    
set sql_safe_updates = 1;



###### DATA MART

### Here I create four different datamarts that show the versatility of the aggreg table

# The table for the linear regression to see is director or actor playing a more important
# role in the film's popular assessment (I set the number of films for each dir/act 
# equal or higher than 5 to avoid high correlation.

drop view if exists avg_actdir_scores;
create view avg_actdir_scores as
	select movieid, title, film_score, act_score, dir_score
		from aggreg
		where  act_films > 4 and dir_films > 4;
select * from avg_actdir_scores;

# A small data mart that aggregates all films and shows if men or women in leading role
# are better assessed.

drop view if exists mf_lead_avg_scores;
create view mf_lead_avg_scores as
	select actor_sex, avg(film_score)
		from aggreg
        group by actor_sex;
select * from mf_lead_avg_scores;

# Here I take a look on how the controversy in public about the films is distributed by genre

drop view if exists var_by_genre;
create view var_by_genre as
	select genre, avg(approx_var) as var
		from aggreg
        group by genre
        having not genre=''
        order by var;
select * from var_by_genre;

# And here I create a suggestion of the best movies from the 80s

drop view if exists best_of_90s;
create view best_of_90s as
	select title, year, film_score, genre, film_votes
		from aggreg
		where year > 1989 and year < 2000 and film_votes > 100000
        order by film_score desc
        limit 20;
select * from best_of_90s;



### Testing the event

select * from aggreg limit 1;
set sql_safe_updates = 0;
update ratings
	set
		`rank` = 8.9,
        votes = 10000000,
        distribution = '0100000008'
	where movieid = 1672052;
set sql_safe_updates = 1;

# Uncomment the next line and run after a couple of minutes to see
# the difference with the previos select:

# select * from aggreg limit 1;