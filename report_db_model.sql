#ASpace-Preservica Reporting DB

DROP DATABASE IF EXISTS `aspace_preservica_db`;

CREATE DATABASE `aspace_preservica_db` 
/*!40100 DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci */ 
/*!80016 DEFAULT ENCRYPTION='N' */;

USE `aspace_preservica_db`;

DROP TABLE IF EXISTS `archival_object`;

CREATE TABLE `archival_object` (
	`id` int(11) NOT NULL,
	`title` varchar(8704) NOT NULL,
	`publish` int NOT NULL,
	`level` varchar(255) NOT NULL,
	`create_time` datetime NOT NULL,
	`m_time` datetime NOT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `digital_object`;

CREATE TABLE `digital_object` (
	`id` int(11) NOT NULL,
	`archival_object_id` int(11) NOT NULL,
	`publish` int NOT NULL,
	`create_time` datetime NOT NULL,
	`m_time` datetime NOT NULL,
PRIMARY KEY (`id`),
KEY `archival_object_id` (`archival_object_id`),
CONSTRAINT `digital_object_ibfk_1` FOREIGN KEY (`archival_object_id`) REFERENCES `archival_object` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `collection`;

CREATE TABLE `collection` (
	`id` varchar(255) NOT NULL,
	`create_time` datetime NOT NULL,
	`m_time` datetime NOT NULL,
PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `deliverable_unit`;

CREATE TABLE `deliverable_unit` (
	`id` varchar(255) NOT NULL,
	`collection_id` varchar(255) NOT NULL,
	`digital_object_id` int(11) DEFAULT NULL,
	`create_time` datetime NOT NULL,
	`m_time` datetime NOT NULL,
PRIMARY KEY (`id`),
KEY `collection_id` (`collection_id`),
KEY `digital_object_id` (`digital_object_id`),
CONSTRAINT `deliverable_unit_ibfk_1` FOREIGN KEY (`collection_id`) REFERENCES `collection` (`id`),
CONSTRAINT `deliverable_unit_ibfk_1` FOREIGN KEY (`digital_object_id`) REFERENCES `digital_object` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `manifestation`;

CREATE TABLE `manifestation` (
	`id` varchar(255) NOT NULL,
	`deliverable_unit_id` varchar(255) NOT NULL,
	`type` varchar(255) DEFAULT NULL,
	`create_time` datetime NOT NULL,
	`m_time` datetime NOT NULL,
PRIMARY KEY (`id`),
KEY `deliverable_unit_id` (`deliverable_unit_id`),
CONSTRAINT `manifestation_ibfk_1` FOREIGN KEY (`deliverable_unit_id`) REFERENCES `deliverable_unit` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `digital_file`;

CREATE TABLE `digital_file` (
	`id` varchar(255) NOT NULL,
	`manifestation_id` varchar(255) NOT NULL,
	`filesize` varchar(255) DEFAULT NULL,
	`metadata` int(11) NOT NULL,
	`content` int(11) NOT NULL,
	`create_time` datetime NOT NULL,
	`m_time` datetime NOT NULL,
PRIMARY KEY (`id`),
KEY `manifestation_id` (`manifestation_id`)
CONSTRAINT `digital_file_ibfk_1` FOREIGN KEY (`manifestation_id`) REFERENCES `manifestation` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

