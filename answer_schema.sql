drop table if exists people_places;

create table `people_places` (
  `id` int not null auto_increment,
  `given_name` varchar(80) default null,
  `family_name` varchar(80) default null,
  `date_of_birth` date,
  `place_of_birth` varchar(80) default null,
  `county` varchar(80) default null,
  `country` varchar(80) default null,
  primary key (`id`)
);
