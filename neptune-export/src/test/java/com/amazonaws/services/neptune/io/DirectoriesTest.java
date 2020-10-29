package com.amazonaws.services.neptune.io;

import org.junit.Test;

import static org.junit.Assert.*;

public class DirectoriesTest {

    @Test
    public void replacesForbiddenCharactersInFilename(){
        String filename = "(Person;Staff;Temp\\;Holidays)-works_for-(Admin;Perm;Person)";
        String updated = Directories.fileName(filename, 1);
        assertEquals("(Person_Staff_Temp__Holidays)-works_for-(Admin_Perm_Person)-1", updated);
    }

}