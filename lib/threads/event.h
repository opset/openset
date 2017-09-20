#ifndef RARELOGIC_LIB_THREADS_EVENT_H_
#define RARELOGIC_LIB_THREADS_EVENT_H_

#include "../include/libcommon.h"

#ifndef _MSC_VER
#include <time.h>
#include <sys/time.h>
#endif

namespace openset
{
	namespace Threads
	{
		class Event
		{
		public:
			Event()
			{
#ifdef _MSC_VER
				this->_Event = CreateEvent(NULL, false, false, NULL);
#else
                pthread_mutex_init( &(this->_Mutex), NULL );
                pthread_cond_init( &(this->_Conditional), NULL );
                this->_IsSignaled = false;
#endif
			};

			~Event()
			{
#ifdef _MSC_VER
				CloseHandle(this->_Event);
#else
                pthread_mutex_destroy( &(this->_Mutex) );
                pthread_cond_destroy( &(this->_Conditional) );
#endif
			};

			int32_t Wait()
			{
#ifdef _MSC_VER
				return WaitForSingleObject(this->_Event, INFINITE);
#else
                int32_t ret = 0;

                pthread_mutex_lock( &(this->_Mutex) );
                
                if ( !this->_IsSignaled )
                    ret = pthread_cond_wait( &(this->_Conditional), &(this->_Mutex) );

                this->_IsSignaled = false;
                pthread_mutex_unlock( &(this->_Mutex) );
                
                return ret;
#endif
			};

			int32_t Wait(uint32_t millis)
			{
#ifdef _MSC_VER
				return WaitForSingleObject(this->_Event, millis);
#else
                struct timespec expire;
                struct timeval current;
                uint32_t msecs = millis % 1000;
                uint32_t secs = (uint32_t)(millis / 1000);
                int ret = 0;

                gettimeofday( &current, NULL );

                expire.tv_sec = current.tv_sec + (time_t)secs;
                expire.tv_nsec = (current.tv_usec * 1000) + (msecs * 1000000);

                pthread_mutex_lock( &(this->_Mutex) );
                
                if ( !this->_IsSignaled )
                {
                    ret = pthread_cond_timedwait( 
                        &(this->_Conditional), 
                        &(this->_Mutex), 
                        &expire );
                }

                this->_IsSignaled = false;
                pthread_mutex_unlock( &(this->_Mutex) );
                
                return ret;
#endif
			};

			void Signal()
			{
#ifdef _MSC_VER
				SetEvent(this->_Event);
#else
                pthread_mutex_lock( &(this->_Mutex) );
                this->_IsSignaled = true;
                pthread_cond_signal( &(this->_Conditional) );
                pthread_mutex_unlock( &(this->_Mutex) );
#endif
			};

		private:
#ifdef _MSC_VER
			HANDLE _Event;
#else
            pthread_mutex_t         _Mutex;
            pthread_cond_t          _Conditional;
            volatile bool           _IsSignaled;
#endif

			Event(const Event& event);
			Event& operator=(const Event& event);
		};
	}; // Threads
}; // OpenSet

#endif // RARELOGIC_LIB_THREADS_EVENT_H_
