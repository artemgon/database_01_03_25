create database Airport;
go;

use Airport;
go;

create table Planes (
    PlaneID int identity(1,1) not null primary key,
    PlaneName nvarchar(50) not null check(PlaneName <> '') unique,
    PlaneNumber nvarchar(50) not null unique,
    PlaneType nvarchar(50) not null check(PlaneType <> ''),
    PlaneCapacity int not null check(PlaneCapacity > 0),
    PlaneSpeed int not null check(PlaneSpeed > 0),
    PlaneFuel int not null check(PlaneFuel >= 0),
    PlaneFuelConsumption int not null check(PlaneFuelConsumption > 0)
);
go;

create table Passengers
(
    PassengerID int identity(1,1) not null primary key,
    PassengerName nvarchar(50) not null check(PassengerName <> ''),
    PassengerSurname nvarchar(50) not null check(PassengerSurname <> ''),
    PassengerAge int not null check(PassengerAge > 0),
    PassengerGender nvarchar(1) not null check(PassengerGender = 'M' or PassengerGender = 'F'),
    PassengerNationality nvarchar(50) not null,
    PassengerPassport nvarchar(50) not null,
    PassengerPhone nvarchar(50) not null check(PassengerPhone <> '') unique default 0,
    PassengerEmail nvarchar(50) not null,
);
go;

create table Flights
(
    FlightID int identity(1,1) not null primary key,
    FlightName nvarchar(50) not null check(FlightName <> ''),
    FlightNumber nvarchar(50) not null unique,
    FlightDistance int not null check(FlightDistance > 0),
    FlightIsEconomy bit not null default 1,
    FlightDestination nvarchar(50) not null check(FlightDestination <> ''),
    FlightDeparture nvarchar(50) not null check(FlightDeparture <> ''),
    FlightDate date not null check(FlightDate <> ''),
    FlightDepartureTime time not null check(FlightDepartureTime <> ''),
    PlaneID int not null foreign key references Planes(PlaneID)
);
go;

create table PlanesFlights
(
    PlaneRaceID int identity(1,1) not null primary key,
    FlightID int not null foreign key references Flights(FlightID),
    PlaneID int not null foreign key references Planes(PlaneID)
);
go;

create table PassengersFlights
(
    PassengerFlightID int identity(1,1) not null primary key,
    PlaneFlightID int not null foreign key references PlanesFlights(PlaneRaceID),
    PlaneID int not null foreign key references Planes(PlaneID),
    PassengerID int not null foreign key references Passengers(PassengerID),
    FlightID int not null foreign key references Flights(FlightID)
);
go;

create table Tickets
(
    TicketID int identity(1,1) not null primary key,
    TicketPrice int not null check(TicketPrice > 0),
    TicketDate date not null check(TicketDate <> '') default getdate(),
    PassengerFlightID int not null foreign key references PassengersFlights(PassengerFlightID),
    PassengerID int not null foreign key references Passengers(PassengerID),
    FlightID int not null foreign key references Flights(FlightID)
);
go;

create table TicketSalesLog
(
    LogID int identity(1,1) not null primary key,
    TicketID int not null foreign key references Tickets(TicketID),
    PassengerID int not null foreign key references Passengers(PassengerID),
    FlightID int not null foreign key references Flights(FlightID),
    TicketPrice int not null check(TicketPrice > 0),
    LogDate date not null check(LogDate <> '') default getdate()
);
go;

create table TicketClasses
(
    ClassID int identity(1,1) not null primary key,
    ClassName nvarchar(20) not null check(ClassName in ('Economy', 'Business', 'First Class')),
    ClassPriceMultiplier float not null check(ClassPriceMultiplier > 0)
);
go;

alter table Tickets
add ClassID int not null foreign key references TicketClasses(ClassID);
go;

create table FlightClassAvailability
(
    AvailabilityID int identity(1,1) not null primary key,
    FlightID int not null foreign key references Flights(FlightID),
    ClassID int not null foreign key references TicketClasses(ClassID),
    AvailableSeats int not null check(AvailableSeats >= 0)
);
go;

if object_id('trg_prevent_overbooking', 'tr') is not null
    drop trigger trg_prevent_overbooking;
go;

-- 1-st trigger that prevents overbooking --
create trigger trg_prevent_overbooking
on PassengersFlights
after insert
as
begin
    if exists (
        select p.planeid
        from inserted i
        join planes p on i.planeid = p.planeid
        group by p.planeid, p.planecapacity
        having count(*) > p.planecapacity
    )
    begin
        raiserror('Cannot add more passengers than plane capacity', 16, 1);
        rollback transaction;
    end;
end;
go;

-- 2-nd trigger that updates plane fuel --
if object_id('trg_update_plane_fuel', 'tr') is not null
    drop trigger trg_update_plane_fuel;
go;

create trigger trg_update_plane_fuel
on Flights
after update
as
begin
    update p
    set planefuel = p.planefuel - (f.flightdistance / p.planefuelconsumption)
    from planes p
    join inserted i on p.planeid = i.planeid
    join flights f on i.flightid = f.flightid;
end;
go;

-- 3-rd trigger that prevents duplicate passengers --
if object_id('trg_prevent_duplicate_passengers', 'tr') is not null
    drop trigger trg_prevent_duplicate_passengers;
go;

create trigger trg_prevent_duplicate_passengers
on PassengersFlights
instead of insert
as
begin
    if exists (
        select 1
        from inserted i
        inner join PassengersFlights pf
            on i.passengerid = pf.passengerid
            and i.flightid = pf.flightid
    )
    begin
        raiserror('Cannot add duplicate passengers: passenger already exists on this flight.', 16, 1);
        rollback transaction;
    end
    else
    begin
        insert into PassengersFlights (PlaneFlightID, PlaneID, PassengerID, FlightID)
        select PlaneFlightID, PlaneID, PassengerID, FlightID
        from inserted;
    end
end;
go;

-- 4-th trigger that logs ticket purchase --
create trigger trg_LogTicketPurchase
on Tickets
after insert
as
begin
    insert into TicketSalesLog(TicketID, PassengerID, FlightID, TicketPrice)
    select TicketID, PassengerID, FlightID, TicketPrice
    from inserted;
end;
go;

-- 5-th trigger that prevents ticket sales for full flights --
create trigger trg_PreventTicketSalesForFullFlights
on Tickets
for insert
as
begin
    if exists
    (
        select 1
        from inserted i
        join Flights f on i.FlightID = f.FlightID
        join Planes p on f.PlaneID = p.PlaneID
        join PassengersFlights pf on f.FlightID = pf.FlightID
        group by p.PlaneCapacity
        having count(pf.PassengerID) >= p.PlaneCapacity
    )
    begin
        raiserror('Cannot sell tickets for full flights', 16, 1);
        rollback transaction;
    end;
end;
go;

-- adding a column for tracking the number of flights a passenger has ever taken --
alter table Passengers
add FlightHistory int not null default 0;
go;

-- 6-th trigger that updates passenger flight history --
create trigger trg_UpdatePassengerFlightHistory
on PassengersFlights
after insert
as
begin
    update Passengers
    set FlightHistory = FlightHistory + 1
    where PassengerID in (select PassengerID from inserted);
end;
go;

-- 7-th trigger that updates available seats after ticket purchase --
create trigger trg_UpdateAvailableSeats
on Tickets
after insert
as
begin
    update FlightClassAvailability
    set AvailableSeats = AvailableSeats - 1
    from FlightClassAvailability fca
    join inserted i on fca.FlightID = i.FlightID and fca.ClassID = i.ClassID;
end;
go;

-- 8-th trigger that prevents ticket sales if no seats are available --
create trigger trg_PreventTicketSaleIfNoSeats
on Tickets
for insert
as
begin
    if exists (
        select 1
        from inserted i
        join FlightClassAvailability fca on i.FlightID = fca.FlightID and i.ClassID = fca.ClassID
        where fca.AvailableSeats <= 0
    )
    begin
        raiserror('Cannot sell ticket: No available seats in this class.', 16, 1);
        rollback transaction;
    end;
end;
go;

-- Add FlightDuration column to Flights table --
alter table Flights
add FlightDuration time not null check(FlightDuration > '00:00:00');
go;

insert into Planes (PlaneName, PlaneNumber, PlaneType, PlaneCapacity, PlaneSpeed, PlaneFuel, PlaneFuelConsumption)
values
    ('Boeing 737', 'BOE-737-100', 'Passenger', 150, 850, 20000, 3000),
    ('Airbus A320', 'AIR-A320-200', 'Passenger', 180, 870, 22000, 2800),
    ('Boeing 777', 'BOE-777-300', 'Cargo', 50, 900, 30000, 4000);
go;

insert into Passengers (PassengerName, PassengerSurname, PassengerAge, PassengerGender, PassengerNationality, PassengerPassport, PassengerPhone, PassengerEmail)
values
    ('John', 'Doe', 30, 'M', 'USA', 'US123456', '123-456-7890', 'john.doe@example.com'),
    ('Alice', 'Smith', 25, 'F', 'Canada', 'CA789012', '987-654-3210', 'alice.smith@example.com'),
    ('Bob', 'Brown', 45, 'M', 'UK', 'UK456789', '555-123-4567', 'bob.brown@example.com');
go;

insert into TicketClasses (ClassName, ClassPriceMultiplier)
values
    ('Economy', 1.0),
    ('Business', 2.5),
    ('First Class', 4.0);
go;

insert into Flights (FlightName, FlightNumber, FlightDistance, FlightIsEconomy, FlightDestination, FlightDeparture, FlightDate, FlightDepartureTime, PlaneID, FlightDuration)
values
    ('NYC Express', 'FL-100', 5000, 1, 'New York', 'Kyiv', '2023-12-25', '08:00:00', 1, '08:30:00'),
    ('London Direct', 'FL-200', 6000, 0, 'London', 'Kyiv', '2023-12-25', '12:00:00', 2, '10:15:00'),
    ('Paris Cargo', 'FL-300', 4500, 1, 'Paris', 'Kyiv', '2023-12-26', '15:30:00', 3, '06:45:00');
go;

insert into PlanesFlights (FlightID, PlaneID)
values
    (1, 1),
    (2, 2),
    (3, 3);
go;

insert into FlightClassAvailability (FlightID, ClassID, AvailableSeats)
values
    (1, 1, 50),  -- Economy для рейсу 1
    (1, 2, 10),  -- Business для рейсу 1
    (2, 1, 30),  -- Economy для рейсу 2
    (2, 3, 5);   -- First Class для рейсу 2
go;

insert into PassengersFlights (PlaneFlightID, PlaneID, PassengerID, FlightID)
values
    (1, 1, 1, 1),
    (1, 1, 2, 1),
    (2, 2, 3, 2);
go;

insert into FlightClassAvailability (FlightID, ClassID, AvailableSeats)
values
    (1, 1, 50),  -- Economy для рейсу 1
    (1, 2, 10),  -- Business для рейсу 1
    (2, 1, 30),  -- Economy для рейсу 2
    (2, 3, 5);   -- First Class для рейсу 2
go;

insert into Tickets (TicketPrice, TicketDate, PassengerFlightID, PassengerID, FlightID, ClassID)
values
    (500, '2023-12-25', 1, 1, 1, 1),  -- Economy
    (1250, '2023-12-25', 2, 2, 1, 2), -- Business
    (2000, '2023-12-25', 3, 3, 2, 3); -- First Class
go;

delete from FlightClassAvailability where AvailabilityID in (5,6,7,8);  -- Remove duplicates
go;

-- 1-st query --
select *
from Flights
where FlightDestination = 'New York' and FlightDate = '2023-12-25'  -- 2021 → 2023
order by FlightDepartureTime;

-- 2-nd query --
select top 1 *
from Flights
order by FlightDuration desc;

-- 3-rd query --
select *
from Flights
where FlightDuration > '02:00:00';

-- 4-th query --
select FlightDestination, count(*) as NumberOfFlights
from Flights
group by FlightDestination;

-- 5-th query --
select top 1 FlightDestination, count(*) as NumberOfFlights
from Flights
group by FlightDestination
order by NumberOfFlights desc;

-- 6-th query --
select FlightDestination, count(*) as NumberOfFlights
from Flights
where month(FlightDate) = 12 and year(FlightDate) = 2021
group by FlightDestination;

-- 7-th query --
select f.*
from Flights f
join FlightClassAvailability fca on f.FlightID = fca.FlightID
join TicketClasses tc on fca.ClassID = tc.ClassID
where f.FlightDate = cast(getdate() as date)
  and tc.ClassName = 'Business'
  and fca.AvailableSeats > 0;

-- 8-th query --
select f.FlightID, count(t.TicketID) as TicketsSold, sum(t.TicketPrice) as TotalRevenue
from Flights f
join Tickets t on f.FlightID = t.FlightID
where f.FlightDate = '2023-12-25'
group by f.FlightID;

-- 9-th query --
select f.FlightID, f.FlightDestination, count(t.TicketID) as TicketsSold
from Flights f
left join Tickets t on f.FlightID = t.FlightID
where f.FlightDate = '2023-12-25'
group by f.FlightID, f.FlightDestination;

-- 10-th query --
select FlightNumber, FlightDestination
from Flights;

drop database Airport;