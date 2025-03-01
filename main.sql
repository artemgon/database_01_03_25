create database Airport;
go;

use Airport;
go;

create table Planes
(
    PlaneID int identity(1,1) not null primary key,
    PlaneName nvarchar(50) not null check(PlaneName <> '') unique,
    PlaneNumber nvarchar(50) not null unique,
    PlaneType nvarchar(50) not null check(PlaneType <> ''),
    PlaneCapacity int not null check(PlaneCapacity > 0),
    PlaneSpeed int not null check(PlaneSpeed > 0),
    PlaneFuel int not null check(PlaneFuel >= 0),
    PlaneFuelConsumption int not null check(PlaneFuelConsumption > 0),
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

-- 1-st trigger that prevents overbooking --
create trigger trg_PreventOverbooking
on PassengersFlights
after insert
as
begin
    if exists
    (
        select 1
        from inserted i
        join Planes P on i.PlaneID = P.PlaneID
        join Flights F on i.FlightID = F.FlightID
        group by p.PlaneCapacity
        having count(i.PassengerID) > p.PlaneCapacity
    )
    begin
        raiserror('Cannot add more passengers than plane capacity', 16, 1);
        rollback transaction;
    end;
end;
go;

-- 2-nd trigger that updates plane fuel --
create trigger trg_UpdatePlaneFuel
on Flights
after update
    as
    begin
        update Planes
        set PlaneFuel = PlaneFuel - (f.FlightDistance / p.PlaneFuelConsumption)
        from planes p
        join inserted i on p.PlaneID = i.PlaneID
        join Flights f on i.FlightID = f.FlightID
    end;
go;

-- 3-rd trigger that prevents duplicate passengers --
create trigger trg_PreventDuplicatePassengers
on PassengersFlights
for insert
as
begin
    if exists
    (
        select 1
        from inserted i
        join PassengersFlights pf on i.PassengerID = pf.PassengerID
        join Flights f on i.FlightID = f.FlightID
    )
    begin
        raiserror('Cannot add duplicate passengers: passenger already exists', 16, 1);
        rollback transaction;
    end;
end;

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

-- 1-st query --
select *
from Flights f
where f.FlightDestination = 'New York' and f.FlightDate = '2021-12-25' and f.FlightDepartureTime = '08:00:00';
