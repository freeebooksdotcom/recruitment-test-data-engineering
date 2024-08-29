drop table if exists places;

create table `places` (
  `id` int not null auto_increment,
  `city` varchar(80) not null,
  `county` varchar(80) default null,
  `country` varchar(80) default null,
  primary key (`id`)
);

drop table if exists people;

create table `people` (
  `id` int not null auto_increment,
  `given_name` varchar(80) default null,
  `family_name` varchar(80) default null,
  `date_of_birth` date,
  `place_of_birth` varchar(80) default null,
  primary key (`id`)
);
